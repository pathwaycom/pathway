use itertools::Itertools;
use log::{error, info, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::collections::HashSet;
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
use crate::connectors::data_format::{parse_bincoded_value, FormatterContext};
use crate::connectors::data_storage::{
    CommitPossibility, ConversionError, SqlQueryTemplate, TableWriterInitMode, ValuesMap,
};
use crate::connectors::metadata::PostgresMetadata;
use crate::connectors::{
    DataEventType, OffsetKey, OffsetValue, ReadError, ReadResult, Reader, ReaderContext,
    StorageType, WriteError, Writer,
};
use crate::engine::{
    DateTimeNaive, DateTimeUtc, Duration as EngineDuration, Key, KeyImpl, Type, Value,
};
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
    use std::ops::Deref;

    use bytes::BytesMut;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use half::f16;
    use numpy::Ix1;
    use ordered_float::OrderedFloat;
    use pgvector::{HalfVector, Vector};
    use postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};

    use crate::engine::time::DateTime as _;
    use crate::engine::Value;

    #[derive(Debug, Clone, thiserror::Error)]
    #[error("cannot convert value of type {pathway_type:?} to Postgres type {postgres_type}")]
    struct WrongPathwayType {
        pathway_type: String,
        postgres_type: Type,
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
            #[allow(clippy::match_same_arms)]
            let pathway_type = match self {
                Self::None => return Ok(IsNull::Yes),
                Self::Bool(b) => {
                    try_forward!(bool, *b);
                    "bool"
                }
                Self::Int(i) => {
                    try_forward!(i64, *i);
                    try_forward!(i32, *i);
                    try_forward!(i16, *i);
                    try_forward!(i8, *i);
                    #[allow(clippy::cast_precision_loss)]
                    {
                        try_forward!(f64, *i as f64);
                        try_forward!(f32, *i as f32);
                    }
                    "int"
                }
                Self::Float(OrderedFloat(f)) => {
                    try_forward!(f64, *f);
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        try_forward!(f32, *f as f32);
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
                #[allow(clippy::cast_possible_truncation)]
                Self::FloatArray(a) => {
                    // TODO regular arrays
                    if a.ndim() == 1 {
                        let v = a
                            .deref()
                            .clone()
                            .into_dimensionality::<Ix1>()
                            .expect("ndim == 1")
                            .to_vec();
                        let v_32: Vec<f32> = v.iter().map(|e| *e as f32).collect();
                        try_forward!(Vector, Vector::from(v_32));
                        let v_16: Vec<f16> = v.iter().map(|e| f16::from_f64(*e)).collect();
                        try_forward!(HalfVector, HalfVector::from(v_16));
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
            Err(Box::new(WrongPathwayType {
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
            Type::Optional(wrapped) | Type::List(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }

                let wrapped = Self::postgres_data_type(wrapped, true)?;
                if let Type::Optional(_) = type_ {
                    return Ok(wrapped);
                }
                format!("{wrapped}[]{not_null_suffix}")
            }
            Type::Tuple(fields) => {
                let mut iter = fields.iter();
                if !fields.is_empty() && iter.all(|field| field == &fields[0]) {
                    let first = Self::postgres_data_type(&fields[0], true)?;
                    return Ok(format!("{first}[]{not_null_suffix}"));
                }
                return Err(WriteError::UnsupportedType(type_.clone()));
            }
            Type::Any | Type::Array(_, _) | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()))
            }
        })
    }
}

mod from_sql {
    use std::error::Error;

    use bytes::Buf;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use half::f16;
    use ndarray::ArrayD;
    use ordered_float::OrderedFloat;
    use pgvector::{HalfVector, Vector};
    use postgres::types::{FromSql, Type};

    use crate::engine::time::{DateTimeNaive, DateTimeUtc};
    use crate::engine::{Duration, Value};

    #[derive(Debug, thiserror::Error)]
    #[error("cannot convert Postgres type {postgres_type} to a Pathway value")]
    struct UnsupportedPostgresType {
        postgres_type: Type,
    }

    fn int_array(ints: Vec<i64>) -> Result<Value, Box<dyn Error + Sync + Send>> {
        let len = ints.len();
        let arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[len]), ints)?;
        Ok(arr.into())
    }

    fn float_array(floats: Vec<f64>) -> Result<Value, Box<dyn Error + Sync + Send>> {
        let len = floats.len();
        let arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[len]), floats)?;
        Ok(arr.into())
    }

    impl<'a> FromSql<'a> for Value {
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

            // chrono timestamps
            if <DateTime<Utc> as FromSql>::accepts(ty) {
                return Ok(Value::DateTimeUtc(DateTimeUtc::from(
                    DateTime::<Utc>::from_sql(ty, raw)?,
                )));
            }
            if <NaiveDateTime as FromSql>::accepts(ty) {
                return Ok(Value::DateTimeNaive(DateTimeNaive::from(
                    NaiveDateTime::from_sql(ty, raw)?,
                )));
            }

            // float arrays
            if <Vec<f64> as FromSql>::accepts(ty) {
                return float_array(Vec::<f64>::from_sql(ty, raw)?);
            }
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

            // int arrays from biggest to smallest
            if <Vec<i64> as FromSql>::accepts(ty) {
                return int_array(Vec::<i64>::from_sql(ty, raw)?);
            }
            if <Vec<i32> as FromSql>::accepts(ty) {
                return int_array(
                    Vec::<i32>::from_sql(ty, raw)?
                        .into_iter()
                        .map(i64::from)
                        .collect(),
                );
            }
            if <Vec<i16> as FromSql>::accepts(ty) {
                return int_array(
                    Vec::<i16>::from_sql(ty, raw)?
                        .into_iter()
                        .map(i64::from)
                        .collect(),
                );
            }

            Err(Box::new(UnsupportedPostgresType {
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
    Native(#[from] pg_walstream::error::ReplicationError),

    #[error("failed to create the initial snapshot")]
    SnapshotNotCreated,
}

pub struct WalReader {
    runtime: TokioRuntime,
    stream: LogicalReplicationStream,
    cancel_token: CancellationToken,
    schema_name: String,
    table_name: String,
    effective_snapshot_name: String,
}

#[derive(Debug)]
pub enum PgEventAfterParsing {
    SimpleChange(ReadResult),
    Truncate,
}

impl WalReader {
    fn new(
        settings: ReplicationSettings,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Self, ReadError> {
        let runtime = create_async_tokio_runtime()?;

        let config = ReplicationStreamConfig::new(
            settings
                .replication_slot_name
                .unwrap_or_else(|| Self::generate_replication_slot_name(schema_name, table_name))
                .clone(),
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
            .or_else(|| stream.exported_snapshot_name().map(ToOwned::to_owned))
            .ok_or(ReplicationError::SnapshotNotCreated)?;

        Ok(Self {
            runtime,
            stream,
            cancel_token,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
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
            .to_string()
            .split('-')
            .next()
            .unwrap()
            .to_string();
        format!("pathway_replication_{schema_name}_{table_name}_{tag}")
    }

    #[allow(clippy::too_many_lines)]
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
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    return Ok(Value::Int(i));
                } else if let Some(f) = n.as_f64() {
                    return Ok(Value::Float(OrderedFloat(f)));
                }
                return Err(err("Failed to parse number".to_string()));
            }
            _other => {
                return Err(err("Unexpected value type".to_string()));
            }
        };

        let value = match type_ {
            Type::Bool => match s {
                "t" | "true" | "TRUE" | "1" => Value::Bool(true),
                "f" | "false" | "FALSE" | "0" => Value::Bool(false),
                _ => return Err(err(format!("Cannot parse bool: {s}"))),
            },
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
            Type::Bytes => {
                let hex = s
                    .strip_prefix("\\x")
                    .ok_or_else(|| err(format!("Bytes not in \\x format: {s}")))?;
                let bytes = hex::decode(hex)
                    .map_err(|e| err(format!("Cannot decode hex bytes '{hex}': {e}")))?;
                Value::Bytes(bytes.into())
            }
            Type::Pointer => {
                let uuid_str = s.replace('-', "");
                let raw = u128::from_str_radix(&uuid_str, 16)
                    .map_err(|e| err(format!("Cannot parse UUID '{s}': {e}")))?;
                Value::Pointer(Key(raw as KeyImpl))
            }
            Type::DateTimeNaive => {
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err(|e| err(format!("Cannot parse DateTimeNaive '{s}': {e}")))?;
                Value::DateTimeNaive(DateTimeNaive::from(dt))
            }
            Type::DateTimeUtc => {
                let dt = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
                    .map_err(|e| err(format!("Cannot parse DateTimeUtc '{s}': {e}")))?;
                Value::DateTimeUtc(DateTimeUtc::from(dt))
            }
            Type::Duration => {
                let d = Self::parse_postgres_interval(s)
                    .map_err(|e| err(format!("Cannot parse Duration '{s}': {e}")))?;
                Value::Duration(d)
            }
            Type::Json => {
                let json: JsonValue = serde_json::from_str(s)
                    .map_err(|e| err(format!("Cannot parse JSON '{s}': {e}")))?;
                json.into()
            }
            Type::Array(_, element_type) => {
                let inner = s
                    .strip_prefix('{')
                    .and_then(|s| s.strip_suffix('}'))
                    .ok_or_else(|| err(format!("Array not in {{...}} format: {s}")))?;

                if inner.is_empty() {
                    match element_type.as_ref() {
                        Type::Int => {
                            return Ok(ArrayD::from_elem(ndarray::IxDyn(&[0]), 0i64).into())
                        }
                        Type::Float => {
                            return Ok(ArrayD::from_elem(ndarray::IxDyn(&[0]), 0f64).into())
                        }
                        _ => {
                            return Err(err(format!(
                                "Unsupported array element type: {element_type}"
                            )))
                        }
                    }
                }
                let parts: Vec<&str> = inner.split(',').collect();
                match element_type.as_ref() {
                    Type::Int => {
                        let nums: Result<Vec<i64>, _> =
                            parts.iter().map(|p| p.trim().parse::<i64>()).collect();
                        let nums =
                            nums.map_err(|e| err(format!("Cannot parse int array '{s}': {e}")))?;
                        let len = nums.len();
                        let arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[len]), nums)
                            .map_err(|e| err(format!("Cannot build int array: {e}")))?;
                        arr.into()
                    }
                    Type::Float => {
                        let nums: Result<Vec<f64>, _> =
                            parts.iter().map(|p| p.trim().parse::<f64>()).collect();
                        let nums =
                            nums.map_err(|e| err(format!("Cannot parse float array '{s}': {e}")))?;
                        let len = nums.len();
                        let arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[len]), nums)
                            .map_err(|e| err(format!("Cannot build float array: {e}")))?;
                        arr.into()
                    }
                    _ => {
                        return Err(err(format!(
                            "Unsupported array element type: {element_type}"
                        )))
                    }
                }
            }
            Type::Tuple(element_types) => {
                let json: JsonValue = serde_json::from_str(s)
                    .map_err(|e| err(format!("Cannot parse Tuple JSON '{s}': {e}")))?;
                let items = match &json {
                    JsonValue::Array(arr) => arr
                        .iter()
                        .zip(element_types.iter())
                        .map(|(v, t)| Self::parse_value_from_postgres(v, t, field_name))
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => return Err(err(format!("Tuple expected JSON array, got: {json}"))),
                };
                Value::Tuple(items.into())
            }
            Type::Optional(inner_type) => {
                Self::parse_value_from_postgres(json_val, inner_type, field_name)?
            }
            Type::PyObjectWrapper => {
                let value = parse_bincoded_value(s)
                    .map_err(|e| err(format!("Cannot deserialize PyObjectWrapper '{s}': {e}")))?;
                match value {
                    Value::PyObjectWrapper(wrapper) => Value::PyObjectWrapper(wrapper),
                    other => {
                        return Err(err(format!(
                            "Expected PyObjectWrapper after deserialization, got: {other:?}"
                        )))
                    }
                }
            }
            other => return Err(err(format!("Unsupported type for parsing: {other}"))),
        };

        Ok(value)
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
                let parts: Vec<&str> = time_part.split(':').collect();
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
                    total_nanos += (hours * 3_600 + minutes * 60 + secs) * 1_000_000_000 + nanos;
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
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
    ) -> ReaderContext {
        let values_map: ValuesMap = value_fields
            .iter()
            .map(|field| {
                let json_val = raw.get(&field.name).unwrap_or(&JsonValue::Null);
                let result = Self::parse_value_from_postgres(json_val, &field.type_, &field.name);
                (field.name.clone(), result)
            })
            .collect::<HashMap<_, _>>()
            .into();

        let key = key_field_names.map(|key_names| {
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
        schema_name: &str,
        table_name: &str,
        event: ChangeEvent,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
    ) -> Vec<PgEventAfterParsing> {
        let event_type = if matches!(event.event_type, EventType::Delete { .. }) {
            DataEventType::Delete
        } else {
            DataEventType::Insert
        };

        match event.event_type {
            EventType::Begin { transaction_id, .. } => vec![PgEventAfterParsing::SimpleChange(
                ReadResult::NewSource(PostgresMetadata::new(Some(transaction_id)).into()),
            )],
            EventType::Commit { .. } => vec![PgEventAfterParsing::SimpleChange(
                ReadResult::FinishedSource {
                    commit_possibility: CommitPossibility::Possible,
                },
            )],
            EventType::Truncate(tables_from_event) => {
                let expected_full_name = format!("{schema_name}.{table_name}").to_string();
                let mut has_target_table = false;
                for table_from_event in tables_from_event {
                    if *table_from_event == expected_full_name {
                        has_target_table = true;
                        break;
                    }
                }
                if !has_target_table {
                    return Vec::new();
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
                if *table_from_event != *table_name || *schema_from_event != *schema_name {
                    return Vec::new();
                }
                let mut result = Vec::with_capacity(2);
                if let Some(old_data) = old_data {
                    result.push(PgEventAfterParsing::SimpleChange(ReadResult::Data(
                        Self::parse_values_from_event(
                            DataEventType::Delete,
                            &old_data.into_hash_map(),
                            value_fields,
                            key_field_names,
                        ),
                        (OffsetKey::Empty, OffsetValue::Empty),
                    )));
                }
                result.push(PgEventAfterParsing::SimpleChange(ReadResult::Data(
                    Self::parse_values_from_event(
                        DataEventType::Insert,
                        &new_data.into_hash_map(),
                        value_fields,
                        key_field_names,
                    ),
                    (OffsetKey::Empty, OffsetValue::Empty),
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
                if *table_from_event != *table_name || *schema_from_event != *schema_name {
                    return Vec::new();
                }
                let ctx = Self::parse_values_from_event(
                    event_type,
                    &data.into_hash_map(),
                    value_fields,
                    key_field_names,
                );
                vec![PgEventAfterParsing::SimpleChange(ReadResult::Data(
                    ctx,
                    (OffsetKey::Empty, OffsetValue::Empty),
                ))]
            }
            _ => Vec::new(),
        }
    }

    fn get_next_records(
        &mut self,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
    ) -> Result<Vec<PgEventAfterParsing>, ReadError> {
        self.runtime.block_on(async {
            loop {
                match self.stream.next_event_with_retry(&self.cancel_token).await {
                    Ok(event) => {
                        self.stream
                            .shared_lsn_feedback
                            .update_applied_lsn(event.lsn.value());
                        let parsed_events = Self::parse_incoming_event(
                            &self.schema_name,
                            &self.table_name,
                            event,
                            value_fields,
                            key_field_names,
                        );
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
    prepared_records: Vec<ReadResult>,
    wal_reader: Option<WalReader>,
    value_fields: Vec<ValueField>,
    key_field_names: Option<Vec<String>>,
    collection_keys: HashSet<Option<Vec<Value>>>,
}

impl PsqlReader {
    pub fn new(
        mut client: PsqlClient,
        replication_settings: Option<ReplicationSettings>,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
    ) -> Result<Self, ReadError> {
        let mut wal_reader = if let Some(replication_settings) = replication_settings {
            Some(WalReader::new(
                replication_settings,
                schema_name,
                table_name,
            )?)
        } else {
            None
        };

        let prepared_records = Self::read_prepared_records(
            &mut client,
            schema_name,
            table_name,
            wal_reader
                .as_ref()
                .map(|wr| wr.effective_snapshot_name.clone()),
            value_fields,
            key_field_names,
        )?;

        if let Some(ref mut wal_reader) = wal_reader {
            wal_reader.start_replication()?;
        }

        Ok(Self {
            prepared_records,
            wal_reader,
            value_fields: value_fields.to_vec(),
            key_field_names: key_field_names.map(<[std::string::String]>::to_vec),
            collection_keys: HashSet::new(),
        })
    }

    fn read_prepared_records(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        snapshot_name: Option<String>,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
    ) -> Result<Vec<ReadResult>, ReadError> {
        let has_txn = snapshot_name.is_some();
        if let Some(snapshot_name) = snapshot_name {
            client.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;", &[])?;
            if let Err(e) =
                client.execute(&format!("SET TRANSACTION SNAPSHOT '{snapshot_name}';"), &[])
            {
                error!("The specified snapshot is no longer available");
                return Err(ReadError::Postgres(e));
            }
        }
        let prepared_records = Self::select_prepared_records(
            client,
            schema_name,
            table_name,
            value_fields,
            key_field_names,
        )?;
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

        let expected_type_unopt = value_field.type_.unoptionalize();
        if *expected_type_unopt == Type::PyObjectWrapper {
            let Value::Bytes(ref bytes) = parsed_value else {
                return Err(make_error(
                    &parsed_value,
                    "unexpected type of a postgres column",
                ));
            };
            return bincode::deserialize::<Value>(bytes)
                .map_err(|err| make_error(&parsed_value, &err.to_string()));
        }

        Ok(parsed_value)
    }

    fn select_prepared_records(
        client: &mut PsqlClient,
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
    ) -> Result<Vec<ReadResult>, ReadError> {
        let select_all_query = format!(
            "SELECT {} FROM {schema_name}.{table_name}",
            (value_fields
                .iter()
                .map(|vf| vf.name.clone())
                .collect::<Vec<_>>()
                .join(",")),
        );
        let snapshot = client.query(&select_all_query, &[])?;

        let mut prepared_records = Vec::with_capacity(snapshot.len() + 2);
        prepared_records.push(ReadResult::NewSource(PostgresMetadata::new(None).into()));
        for row in snapshot {
            let mut parsed_row_value = HashMap::with_capacity(row.len());
            for (index, value_field) in value_fields.iter().enumerate().take(row.len()) {
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

            let (key_parts, is_correct) = if let Some(key_field_names) = key_field_names {
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
                (OffsetKey::Empty, OffsetValue::Empty),
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
                    parsed_pg_events = wal_reader
                        .get_next_records(&self.value_fields, self.key_field_names.as_deref())?;
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
                                (OffsetKey::Empty, OffsetValue::Empty),
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

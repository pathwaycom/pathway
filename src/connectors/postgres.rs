use itertools::Itertools;

use native_tls::Error as NativeTlsError;
use native_tls::{Certificate, TlsConnector};
use postgres::types::ToSql;
use postgres::Client as PsqlClient;
use postgres::NoTls;
use postgres_native_tls::MakeTlsConnector;

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::{SqlQueryTemplate, TableWriterInitMode};
use crate::connectors::{WriteError, Writer};
use crate::engine::{Type, Value};
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

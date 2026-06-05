// Copyright © 2026 Pathway

use std::collections::HashMap;

use chrono::TimeZone;
use chrono_tz_0_8::Tz;
use clickhouse_rs::types::column::ColumnFrom;
use clickhouse_rs::{errors::Error as ClickHouseClientError, Block, ClientHandle, Pool};
use tokio::runtime::Runtime as TokioRuntime;

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::{create_bincoded_value, FormatterContext};
use crate::connectors::data_storage::TableWriterInitMode;
use crate::connectors::{SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::time::DateTime as DateTimeTrait;
use crate::engine::{Type, Value};
use crate::python_api::ValueField;

use super::{WriteError, Writer};

/// Errors specific to the `ClickHouse` output connector.
///
/// A dedicated enum (rather than surfacing the raw driver error everywhere)
/// lets the engine give the user actionable messages for the two
/// failure modes they can actually act on — an unrepresentable value and a
/// connection/insert failure — while every other driver error is forwarded
/// transparently.
#[derive(Debug, thiserror::Error)]
pub enum ClickHouseError {
    /// Any error reported by the underlying native-protocol driver: a failed
    /// connection, a rejected DDL statement, a type mismatch detected by the
    /// server on insert, etc.
    #[error(transparent)]
    Driver(#[from] ClickHouseClientError),

    /// A value did not match the Pathway type declared for its column. The
    /// engine guarantees column-type consistency upstream, so this indicates an
    /// internal inconsistency rather than user error.
    #[error("value {0:?} does not match the declared type of column {1:?}")]
    ValueTypeMismatch(Value, String),

    /// The destination table does not exist and `init_mode` does not create it.
    #[error(
        "the ClickHouse table {0:?} does not exist; create it beforehand or pass \
         init_mode=\"create_if_not_exists\" (or \"replace\") so the connector creates it"
    )]
    TableDoesNotExist(String),

    /// The destination table exists but is missing columns the output needs:
    /// the input columns plus the `time` and `diff` metadata columns.
    #[error(
        "the ClickHouse table {table:?} is missing column(s) {missing:?} that the output \
         requires (the input columns plus the 'time' and 'diff' metadata columns); add them \
         to the table, rename the Pathway columns to match, or use init_mode=\"replace\""
    )]
    MissingColumns { table: String, missing: Vec<String> },

    /// A destination column exists but its type is incompatible with the value
    /// the writer produces for it. `ClickHouse` inserts perform almost no implicit
    /// conversion, so the column type must match the writer's type exactly (a
    /// `String` column may also be `FixedString(N)`, and a datetime column may be
    /// any `DateTime`/`DateTime64` variant).
    #[error(
        "column {column:?} in the ClickHouse table {table:?} has type {actual:?}, which is \
         incompatible with the type {expected:?} that the connector inserts into it; change \
         the column to {expected:?} or use init_mode=\"replace\" to recreate the table"
    )]
    IncompatibleColumnType {
        table: String,
        column: String,
        expected: String,
        actual: String,
    },

    /// Snapshot mode was requested without any primary-key column.
    #[error(
        "snapshot mode requires a primary key: pass `primary_key=[...]` so the connector \
         can order the ReplacingMergeTree and collapse each key to its latest version"
    )]
    SnapshotKeyMissing,

    /// A primary-key column is not among the table's columns.
    #[error("the primary-key column {0:?} is not present among the output columns")]
    SnapshotKeyNotAColumn(String),

    /// In snapshot mode the destination table is not a `ReplacingMergeTree`, so
    /// it cannot collapse a primary key to its latest version.
    #[error(
        "snapshot mode requires the ClickHouse table {table:?} to use the \
         ReplacingMergeTree(version, is_deleted) engine, but it uses {engine:?}; recreate the \
         table with that engine or use init_mode=\"replace\""
    )]
    SnapshotWrongEngine { table: String, engine: String },
}

/// Returns whether the writer's value of `ClickHouse` type `expected` can be
/// inserted into a destination column of type `actual`.
///
/// `ClickHouse`'s native-protocol insert performs almost no implicit conversion:
/// the types must match exactly, with two exceptions exercised by this
/// connector — a `String` value goes into a `FixedString(N)` column, and a
/// datetime value (always produced as `DateTime64(9, 'UTC')`) goes into any
/// `DateTime`/`DateTime64` column. Nullability must match: a non-nullable value
/// cannot be inserted into a `Nullable` column and vice versa.
pub fn clickhouse_type_compatible(expected: &str, actual: &str) -> bool {
    if expected == actual {
        return true;
    }
    match (strip_nullable(expected), strip_nullable(actual)) {
        (Some(expected_inner), Some(actual_inner)) => {
            return clickhouse_type_compatible(expected_inner, actual_inner);
        }
        // Exactly one side is nullable — ClickHouse rejects the insert.
        (Some(_), None) | (None, Some(_)) => return false,
        (None, None) => {}
    }
    match (strip_array(expected), strip_array(actual)) {
        // Both arrays — the element types must themselves be compatible.
        (Some(expected_inner), Some(actual_inner)) => {
            return clickhouse_type_compatible(expected_inner, actual_inner);
        }
        (Some(_), None) | (None, Some(_)) => return false,
        (None, None) => {}
    }
    if expected == "String" && actual.starts_with("FixedString(") {
        return true;
    }
    if expected.starts_with("DateTime") && actual.starts_with("DateTime") {
        return true;
    }
    false
}

/// Strips a `Nullable(...)` wrapper, returning the inner type.
fn strip_nullable(type_: &str) -> Option<&str> {
    type_
        .strip_prefix("Nullable(")
        .and_then(|inner| inner.strip_suffix(')'))
}

/// Strips an `Array(...)` wrapper, returning the element type.
fn strip_array(type_: &str) -> Option<&str> {
    type_
        .strip_prefix("Array(")
        .and_then(|inner| inner.strip_suffix(')'))
}

/// Snapshot-mode metadata column carrying the per-change version. The
/// `ReplacingMergeTree` engine keeps, per primary key, the row with the highest
/// version, so a monotonically increasing version makes later changes win.
const SNAPSHOT_FIELD_VERSION: &str = "version";

/// Snapshot-mode metadata column marking a row as a deletion (`1`) or a state
/// row (`0`). `ReplacingMergeTree(version, is_deleted)` drops a key whose
/// highest-version row has `is_deleted = 1`.
const SNAPSHOT_FIELD_IS_DELETED: &str = "is_deleted";

/// Writer for a `ClickHouse` table, in either stream-of-changes or snapshot mode.
///
/// In **stream-of-changes** mode (the default) every Pathway change is written as
/// one row containing the input columns plus two metadata columns: `time` (the
/// minibatch timestamp) and `diff` (`1` for an insertion, `-1` for a deletion).
/// This mirrors the stream-of-changes output of the other relational sinks.
///
/// In **snapshot** mode the destination is a `ReplacingMergeTree(version,
/// is_deleted)` table ordered by the primary key. `ClickHouse` has no synchronous
/// upsert, so each change is appended as a row carrying a monotonically
/// increasing `version` and an `is_deleted` flag (`1` for a retraction); the
/// engine collapses each primary key to its latest version on merge, and
/// `SELECT … FINAL` returns the current state. Snapshot mode runs on a single
/// worker so the version counter is globally monotonic.
pub struct ClickHouseWriter {
    runtime: TokioRuntime,
    pool: Pool,
    table_name: String,
    value_fields: Vec<ValueField>,
    max_batch_size: Option<usize>,
    buffer: Vec<FormatterContext>,
    snapshot_mode: bool,
    /// Next version to assign in snapshot mode; seeded above the table's current
    /// maximum so changes win over rows from a previous run. The primary-key
    /// columns are not retained — they only shape the table's `ORDER BY` at
    /// creation and are already part of the value columns written on each change.
    next_version: u64,
}

/// Quote a `ClickHouse` identifier with backticks, doubling any internal
/// backtick. Applied to every user-supplied table or column name interpolated
/// into generated DDL so that reserved words and characters requiring quoting
/// survive round-tripping.
fn clickhouse_quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Escapes a value for interpolation into a single-quoted `ClickHouse` SQL string
/// literal (used for the `system.tables` / `system.columns` preflight lookups).
fn escape_sql_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Splits a column type into its non-optional base and whether it is optional.
fn split_optional(type_: &Type) -> (&Type, bool) {
    match type_ {
        Type::Optional(inner) => (inner.as_ref(), true),
        other => (other, false),
    }
}

/// Builds a nanosecond-since-epoch timestamp into a `chrono` datetime in UTC.
///
/// `DateTimeNaive` is interpreted as a UTC wall-clock instant (the same
/// convention used when reading it back), and `DateTimeUtc` already is one, so
/// both round-trip the exact nanosecond instant through `DateTime64(9, 'UTC')`.
fn nanos_to_chrono_utc(nanos: i64) -> chrono::DateTime<Tz> {
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsec_nanos = nanos.rem_euclid(1_000_000_000);
    Tz::UTC
        .timestamp_opt(
            seconds,
            subsec_nanos
                .try_into()
                .expect("the remainder of a division by 10^9 always fits u32"),
        )
        .single()
        .expect("a UTC timestamp is never ambiguous or nonexistent")
}

/// Maps a Pathway column type to the `ClickHouse` column type the connector
/// creates for it. Must stay consistent with the column builders in
/// [`ClickHouseWriter::append_value_column`].
///
/// `bytes` is stored as `Nullable(String)` regardless of optionality because the
/// native-protocol client can only build a byte-valued `String` column through
/// its nullable column builder.
fn clickhouse_data_type(type_: &Type) -> Result<String, WriteError> {
    let (base, optional) = split_optional(type_);
    let base_str = match base {
        Type::Bool => "Bool",
        Type::Int | Type::Duration => "Int64",
        Type::Float => "Float64",
        Type::String | Type::Pointer | Type::Json | Type::PyObjectWrapper => "String",
        Type::DateTimeNaive | Type::DateTimeUtc => "DateTime64(9, 'UTC')",
        Type::Bytes => return Ok("Nullable(String)".to_string()),
        Type::List(element) => {
            // ClickHouse arrays cannot be wrapped in `Nullable`, so `Optional`
            // lists are unsupported; a plain `list[T]` becomes `Array(<T>)`.
            if optional {
                return Err(WriteError::UnsupportedType(type_.clone()));
            }
            return Ok(format!(
                "Array({})",
                clickhouse_array_element_type(element, type_)?
            ));
        }
        // A fixed-arity tuple whose elements all share one supported scalar type
        // is stored as `Array(<T>)`, just like a `list[T]` (ClickHouse `Array` is
        // variable-length, so the arity is not preserved at the type level).
        // Heterogeneous tuples would need a ClickHouse `Tuple`, which the
        // native-protocol client cannot build, so they are unsupported.
        Type::Tuple(elements) => {
            if optional {
                return Err(WriteError::UnsupportedType(type_.clone()));
            }
            let element = tuple_common_element(elements)
                .ok_or_else(|| WriteError::UnsupportedType(type_.clone()))?;
            return Ok(format!(
                "Array({})",
                clickhouse_array_element_type(element, type_)?
            ));
        }
        // Only a one-dimensional, element-typed `int`/`float` ndarray maps to a
        // native `Array(Int64)`/`Array(Float64)` — the native-protocol client can
        // build only one level of array, so higher-dimensional arrays have no
        // `Array(Array(...))` target, and an unknown dimensionality or element
        // type cannot pin down the column type. Arrays cannot be `Nullable`.
        Type::Array(n_dim, element) => {
            if optional || *n_dim != Some(1) {
                return Err(WriteError::UnsupportedType(type_.clone()));
            }
            let element_type = match element.as_ref() {
                Type::Int => "Int64",
                Type::Float => "Float64",
                _ => return Err(WriteError::UnsupportedType(type_.clone())),
            };
            return Ok(format!("Array({element_type})"));
        }
        _ => return Err(WriteError::UnsupportedType(type_.clone())),
    };
    if optional {
        Ok(format!("Nullable({base_str})"))
    } else {
        Ok(base_str.to_string())
    }
}

/// Maps a list's element type to the `ClickHouse` `Array` element type. Only the
/// scalar element types whose array columns the native-protocol client can build
/// at full fidelity are accepted. Datetimes are excluded: the client's
/// array-of-datetime builder is second-precision, which would silently drop the
/// sub-second part Pathway carries. Nullable, byte, and nested (list/tuple/array)
/// elements are unsupported too. `outer` is the full list type, reported in the
/// error for context.
fn clickhouse_array_element_type(element: &Type, outer: &Type) -> Result<&'static str, WriteError> {
    match element {
        Type::Bool => Ok("Bool"),
        Type::Int | Type::Duration => Ok("Int64"),
        Type::Float => Ok("Float64"),
        Type::String | Type::Pointer | Type::Json | Type::PyObjectWrapper => Ok("String"),
        _ => Err(WriteError::UnsupportedType(outer.clone())),
    }
}

/// Returns the single element type shared by every entry of a tuple, or `None`
/// if the tuple is empty or its element types are not all identical (i.e. it is
/// heterogeneous and has no single `Array` element type).
fn tuple_common_element(elements: &[Type]) -> Option<&Type> {
    let first = elements.first()?;
    elements
        .iter()
        .all(|element| element == first)
        .then_some(first)
}

/// Builds the error reported when a value does not fit the Pathway type declared
/// for its column. The engine guarantees column-type consistency upstream, so
/// this is an internal-consistency guard rather than a user-facing error.
fn value_type_mismatch(value: &Value, column_name: &str) -> WriteError {
    WriteError::ClickHouse(ClickHouseError::ValueTypeMismatch(
        value.clone(),
        column_name.to_string(),
    ))
}

/// Collects a column by applying `convert` to every value, failing with a
/// type-mismatch error on the first value `convert` rejects. This is the single
/// primitive the other collectors build on: the non-nullable scalar columns and
/// the (never-nullable) 1-D ndarray columns use it directly — for an ndarray
/// `convert` extracts the flat `Vec<T>` from each `IntArray`/`FloatArray` value —
/// while the nullable and list variants wrap it with extra per-value handling.
fn collect_column<T>(
    values: &[&Value],
    column_name: &str,
    convert: impl Fn(&Value) -> Option<T>,
) -> Result<Vec<T>, WriteError> {
    values
        .iter()
        .map(|value| convert(value).ok_or_else(|| value_type_mismatch(value, column_name)))
        .collect()
}

/// Collects a nullable column. `Value::None` becomes `None`; every other value
/// is passed through `convert`.
fn collect_optional<T>(
    values: &[&Value],
    column_name: &str,
    convert: impl Fn(&Value) -> Option<T>,
) -> Result<Vec<Option<T>>, WriteError> {
    collect_column(values, column_name, |value| match value {
        Value::None => Some(None),
        other => convert(other).map(Some),
    })
}

/// Collects an `Array(T)` column. Each input value is a list (`Value::Tuple`)
/// whose elements are converted with `convert`, producing one inner `Vec<T>` per
/// row — exactly the `Vec<Vec<T>>` the native-protocol client turns into an
/// `Array(T)` column. A non-list value, or a list with an element `convert`
/// rejects, fails with a type-mismatch error naming the whole row value.
fn collect_list<T>(
    values: &[&Value],
    column_name: &str,
    convert: impl Fn(&Value) -> Option<T>,
) -> Result<Vec<Vec<T>>, WriteError> {
    collect_column(values, column_name, |value| match value {
        Value::Tuple(elements) => elements.iter().map(&convert).collect::<Option<Vec<T>>>(),
        _ => None,
    })
}

// Per-type value converters, shared between the scalar columns
// ([`ClickHouseWriter::append_value_column`]) and the `Array` element columns
// ([`ClickHouseWriter::append_list_column`]), which store the same scalar types
// the same way. Each returns the Rust value the matching `ClickHouse` column
// builder expects, or `None` if the value is not of the expected variant.

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(b) => Some(*b),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        _ => None,
    }
}

/// A `Duration` is stored as its nanosecond count in an `Int64` column.
fn value_as_duration_nanos(value: &Value) -> Option<i64> {
    match value {
        Value::Duration(d) => Some(d.nanoseconds()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float(f) => Some(f.0),
        _ => None,
    }
}

/// Converts a string-like value to the `String` the connector stores for it. A
/// `PyObjectWrapper` is stored as the base64-encoded bincode of its `Value`,
/// matching `pw.serialize`, so it round-trips back through `pw.deserialize` after
/// base64 decoding.
fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.to_string()),
        Value::Pointer(p) => Some(p.to_string()),
        Value::Json(j) => Some(j.to_string()),
        Value::PyObjectWrapper(_) => create_bincoded_value(value).ok(),
        _ => None,
    }
}

impl ClickHouseWriter {
    pub fn new(
        connection_string: &str,
        table_name: &str,
        value_fields: Vec<ValueField>,
        max_batch_size: Option<usize>,
        mode: TableWriterInitMode,
        snapshot_mode: bool,
        key_field_names: Option<Vec<String>>,
    ) -> Result<ClickHouseWriter, WriteError> {
        // Validate every column type up front, independent of `init_mode`, so an
        // unsupported type is reported at construction rather than on the first
        // flush. The `init_mode="default"` path runs no DDL, so this is the only
        // place the type would otherwise be checked.
        for field in &value_fields {
            clickhouse_data_type(&field.type_)?;
        }

        // Snapshot mode needs a primary key to order the ReplacingMergeTree, and
        // every key column must be an actual output column.
        let key_field_names = key_field_names.unwrap_or_default();
        if snapshot_mode {
            if key_field_names.is_empty() {
                return Err(ClickHouseError::SnapshotKeyMissing.into());
            }
            for key in &key_field_names {
                if !value_fields.iter().any(|field| &field.name == key) {
                    return Err(ClickHouseError::SnapshotKeyNotAColumn(key.clone()).into());
                }
            }
        }

        let runtime = create_async_tokio_runtime()?;
        let pool = Pool::new(connection_string);

        let quoted_table_name = clickhouse_quote_identifier(table_name);
        let next_version = runtime.block_on(async {
            let mut client = pool.get_handle().await.map_err(ClickHouseError::Driver)?;
            // Surface a misconfigured connection (bad host, credentials, …)
            // eagerly at construction time rather than on the first flush.
            client.ping().await.map_err(ClickHouseError::Driver)?;
            for query in Self::init_queries(
                &quoted_table_name,
                &value_fields,
                mode,
                snapshot_mode,
                &key_field_names,
            )? {
                client
                    .execute(query.as_str())
                    .await
                    .map_err(ClickHouseError::Driver)?;
            }
            // Preflight the destination: it must exist (the init DDL has already
            // run for the creating modes) and every column the writer inserts must
            // be present with a type the writer's values can be inserted into, so a
            // missing table, an absent column, or an incompatible column type
            // surfaces now instead of failing on the first insert.
            Self::validate_destination(&mut client, table_name, &value_fields, snapshot_mode)
                .await?;
            // Seed the version counter above the table's current maximum so that,
            // after a restart or against a pre-populated table, freshly written
            // changes win over the rows already present.
            let next_version = if snapshot_mode {
                Self::fetch_max_version(&mut client, table_name).await? + 1
            } else {
                0
            };
            Ok::<u64, WriteError>(next_version)
        })?;

        Ok(ClickHouseWriter {
            runtime,
            pool,
            table_name: table_name.to_string(),
            value_fields,
            max_batch_size,
            buffer: Vec::new(),
            snapshot_mode,
            next_version,
        })
    }

    /// Produces the DDL statements that the selected init mode runs before the
    /// first write. In stream-of-changes mode the table carries the input columns
    /// plus the `time`/`diff` metadata columns on a plain `MergeTree`. In snapshot
    /// mode it carries the input columns plus the `version`/`is_deleted` metadata
    /// columns on a `ReplacingMergeTree(version, is_deleted)` ordered by the
    /// primary key.
    fn init_queries(
        quoted_table_name: &str,
        value_fields: &[ValueField],
        mode: TableWriterInitMode,
        snapshot_mode: bool,
        key_field_names: &[String],
    ) -> Result<Vec<String>, WriteError> {
        let mut queries = Vec::new();
        match mode {
            TableWriterInitMode::Default => {}
            TableWriterInitMode::CreateIfNotExists | TableWriterInitMode::Replace => {
                if mode == TableWriterInitMode::Replace {
                    queries.push(format!("DROP TABLE IF EXISTS {quoted_table_name}"));
                }
                let mut columns = Vec::with_capacity(value_fields.len() + 2);
                for field in value_fields {
                    columns.push(format!(
                        "{} {}",
                        clickhouse_quote_identifier(&field.name),
                        clickhouse_data_type(&field.type_)?
                    ));
                }
                let engine_and_order = if snapshot_mode {
                    columns.push(format!("{SNAPSHOT_FIELD_VERSION} UInt64"));
                    columns.push(format!("{SNAPSHOT_FIELD_IS_DELETED} UInt8"));
                    let order_by = key_field_names
                        .iter()
                        .map(|name| clickhouse_quote_identifier(name))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "ENGINE = ReplacingMergeTree({SNAPSHOT_FIELD_VERSION}, {SNAPSHOT_FIELD_IS_DELETED}) ORDER BY ({order_by})"
                    )
                } else {
                    columns.push(format!("{SPECIAL_FIELD_TIME} Int64"));
                    columns.push(format!("{SPECIAL_FIELD_DIFF} Int8"));
                    "ENGINE = MergeTree ORDER BY tuple()".to_string()
                };
                queries.push(format!(
                    "CREATE TABLE IF NOT EXISTS {quoted_table_name} ({}) {engine_and_order}",
                    columns.join(", ")
                ));
            }
        }
        Ok(queries)
    }

    /// Verifies that the destination table exists and that every column the
    /// writer inserts is present with a type the writer's values can actually be
    /// inserted into. The metadata columns differ by mode: `time`/`diff` in
    /// stream mode, `version`/`is_deleted` in snapshot mode (which additionally
    /// requires the `ReplacingMergeTree` engine). Run at construction so a missing
    /// table, an absent column, an incompatible column type, or a wrong engine is
    /// reported up front rather than on the first insert.
    async fn validate_destination(
        client: &mut ClientHandle,
        table_name: &str,
        value_fields: &[ValueField],
        snapshot_mode: bool,
    ) -> Result<(), WriteError> {
        // The table name is interpolated into a SQL string literal, so escape
        // backslashes and single quotes the way ClickHouse expects.
        let escaped = escape_sql_string_literal(table_name);
        let query = format!(
            "SELECT name, type FROM system.columns \
             WHERE database = currentDatabase() AND table = '{escaped}'"
        );
        let block = client
            .query(query.as_str())
            .fetch_all()
            .await
            .map_err(ClickHouseError::Driver)?;
        let mut existing: HashMap<String, String> = HashMap::new();
        for row in block.rows() {
            let name: String = row.get("name").map_err(ClickHouseError::Driver)?;
            let type_: String = row.get("type").map_err(ClickHouseError::Driver)?;
            existing.insert(name, type_);
        }
        // A table always has at least one column, so an empty result means it
        // does not exist.
        if existing.is_empty() {
            return Err(ClickHouseError::TableDoesNotExist(table_name.to_string()).into());
        }

        // The columns the writer inserts, paired with the ClickHouse type it
        // produces for each, in the order they would be reported.
        let mut required: Vec<(String, String)> = Vec::with_capacity(value_fields.len() + 2);
        for field in value_fields {
            required.push((field.name.clone(), clickhouse_data_type(&field.type_)?));
        }
        if snapshot_mode {
            required.push((SNAPSHOT_FIELD_VERSION.to_string(), "UInt64".to_string()));
            required.push((SNAPSHOT_FIELD_IS_DELETED.to_string(), "UInt8".to_string()));
        } else {
            required.push((SPECIAL_FIELD_TIME.to_string(), "Int64".to_string()));
            required.push((SPECIAL_FIELD_DIFF.to_string(), "Int8".to_string()));
        }

        let missing: Vec<String> = required
            .iter()
            .filter(|(name, _)| !existing.contains_key(name))
            .map(|(name, _)| name.clone())
            .collect();
        if !missing.is_empty() {
            return Err(ClickHouseError::MissingColumns {
                table: table_name.to_string(),
                missing,
            }
            .into());
        }

        for (name, expected) in &required {
            let actual = &existing[name];
            if !clickhouse_type_compatible(expected, actual) {
                return Err(ClickHouseError::IncompatibleColumnType {
                    table: table_name.to_string(),
                    column: name.clone(),
                    expected: expected.clone(),
                    actual: actual.clone(),
                }
                .into());
            }
        }

        if snapshot_mode {
            let engine = Self::fetch_table_engine(client, table_name).await?;
            if !engine.starts_with("ReplacingMergeTree") {
                return Err(ClickHouseError::SnapshotWrongEngine {
                    table: table_name.to_string(),
                    engine,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Reads the engine family of `table_name` from `system.tables`.
    async fn fetch_table_engine(
        client: &mut ClientHandle,
        table_name: &str,
    ) -> Result<String, WriteError> {
        let escaped = escape_sql_string_literal(table_name);
        let query = format!(
            "SELECT engine FROM system.tables \
             WHERE database = currentDatabase() AND name = '{escaped}'"
        );
        let block = client
            .query(query.as_str())
            .fetch_all()
            .await
            .map_err(ClickHouseError::Driver)?;
        let engine = block
            .rows()
            .next()
            .map(|row| row.get::<String, _>("engine"))
            .transpose()
            .map_err(ClickHouseError::Driver)?
            .unwrap_or_default();
        Ok(engine)
    }

    /// Reads the current maximum value of the snapshot `version` column, or `0`
    /// when the table is empty.
    async fn fetch_max_version(
        client: &mut ClientHandle,
        table_name: &str,
    ) -> Result<u64, WriteError> {
        let query = format!(
            "SELECT ifNull(max({SNAPSHOT_FIELD_VERSION}), toUInt64(0)) AS v FROM {}",
            clickhouse_quote_identifier(table_name)
        );
        let block = client
            .query(query.as_str())
            .fetch_all()
            .await
            .map_err(ClickHouseError::Driver)?;
        let max_version = block
            .rows()
            .next()
            .map(|row| row.get::<u64, _>("v"))
            .transpose()
            .map_err(ClickHouseError::Driver)?
            .unwrap_or(0);
        Ok(max_version)
    }

    /// Appends a non-array scalar column, choosing the nullable or non-nullable
    /// builder by `optional`. `convert` maps each value to the Rust type the
    /// matching `ClickHouse` column builder expects.
    fn append_scalar<T>(
        block: Block,
        name: &str,
        values: &[&Value],
        optional: bool,
        convert: impl Fn(&Value) -> Option<T>,
    ) -> Result<Block, WriteError>
    where
        Vec<T>: ColumnFrom,
        Vec<Option<T>>: ColumnFrom,
    {
        if optional {
            Ok(block.column(name, collect_optional(values, name, convert)?))
        } else {
            Ok(block.column(name, collect_column(values, name, convert)?))
        }
    }

    /// Appends one user column to `block`, dispatching on the column's Pathway
    /// type. The chosen builder type must match the `ClickHouse` type produced by
    /// [`clickhouse_data_type`] so that the server-side cast on insert succeeds.
    fn append_value_column(
        block: Block,
        name: &str,
        type_: &Type,
        values: &[&Value],
    ) -> Result<Block, WriteError> {
        let (base, optional) = split_optional(type_);
        let block = match base {
            Type::Bool => Self::append_scalar(block, name, values, optional, value_as_bool)?,
            Type::Int => Self::append_scalar(block, name, values, optional, value_as_i64)?,
            Type::Duration => {
                Self::append_scalar(block, name, values, optional, value_as_duration_nanos)?
            }
            Type::Float => Self::append_scalar(block, name, values, optional, value_as_f64)?,
            Type::String | Type::Pointer | Type::Json | Type::PyObjectWrapper => {
                Self::append_scalar(block, name, values, optional, value_as_string)?
            }
            Type::DateTimeNaive | Type::DateTimeUtc => {
                Self::append_scalar(block, name, values, optional, |value| match value {
                    Value::DateTimeNaive(dt) => Some(nanos_to_chrono_utc(dt.timestamp())),
                    Value::DateTimeUtc(dt) => Some(nanos_to_chrono_utc(dt.timestamp())),
                    _ => None,
                })?
            }
            // Bytes is always stored as `Nullable(String)` (see
            // `clickhouse_data_type`), so it is always built with the nullable
            // byte-string column builder regardless of declared optionality.
            Type::Bytes => block.column(
                name,
                collect_optional(values, name, |value| match value {
                    Value::Bytes(b) => Some(b.to_vec()),
                    _ => None,
                })?,
            ),
            // A homogeneous `list[T]` is stored as `Array(<T>)`.
            Type::List(element) => Self::append_list_column(block, name, element, values, type_)?,
            // A homogeneous-element tuple is stored as `Array(<T>)`; its values
            // arrive as the same `Value::Tuple` a list does, so it reuses the list
            // column builder with the tuple's common element type.
            Type::Tuple(elements) => {
                let element = tuple_common_element(elements)
                    .ok_or_else(|| WriteError::UnsupportedType(type_.clone()))?;
                Self::append_list_column(block, name, element, values, type_)?
            }
            // A 1-D `int`/`float` ndarray is stored as `Array(Int64)`/`Array(Float64)`.
            Type::Array(_, element) => {
                Self::append_ndarray_column(block, name, element, values, type_)?
            }
            _ => return Err(WriteError::UnsupportedType(type_.clone())),
        };
        Ok(block)
    }

    /// Appends a 1-D ndarray value column as a `ClickHouse`
    /// `Array(Int64)`/`Array(Float64)`. The element type and (one-)dimensionality
    /// were already checked at construction by [`clickhouse_data_type`]; each
    /// value is defensively required to be one-dimensional. `array_type` is the
    /// full ndarray type, reported on mismatch.
    fn append_ndarray_column(
        block: Block,
        name: &str,
        element: &Type,
        values: &[&Value],
        array_type: &Type,
    ) -> Result<Block, WriteError> {
        let block = match element {
            Type::Int => block.column(
                name,
                collect_column(values, name, |v| match v {
                    Value::IntArray(a) if a.ndim() == 1 => {
                        Some(a.iter().copied().collect::<Vec<i64>>())
                    }
                    _ => None,
                })?,
            ),
            Type::Float => block.column(
                name,
                collect_column(values, name, |v| match v {
                    Value::FloatArray(a) if a.ndim() == 1 => {
                        Some(a.iter().copied().collect::<Vec<f64>>())
                    }
                    _ => None,
                })?,
            ),
            _ => return Err(WriteError::UnsupportedType(array_type.clone())),
        };
        Ok(block)
    }

    /// Appends a `list[T]` value column as a `ClickHouse` `Array(<T>)`, dispatching
    /// on the element type to the matching `Vec<Vec<T>>` column builder. Each
    /// list value becomes one inner vector. `list_type` is the full list type,
    /// reported in the error for an unsupported element type.
    fn append_list_column(
        block: Block,
        name: &str,
        element: &Type,
        values: &[&Value],
        list_type: &Type,
    ) -> Result<Block, WriteError> {
        let block = match element {
            Type::Bool => block.column(name, collect_list(values, name, value_as_bool)?),
            Type::Int => block.column(name, collect_list(values, name, value_as_i64)?),
            Type::Duration => {
                block.column(name, collect_list(values, name, value_as_duration_nanos)?)
            }
            Type::Float => block.column(name, collect_list(values, name, value_as_f64)?),
            Type::String | Type::Pointer | Type::Json | Type::PyObjectWrapper => {
                block.column(name, collect_list(values, name, value_as_string)?)
            }
            _ => return Err(WriteError::UnsupportedType(list_type.clone())),
        };
        Ok(block)
    }

    /// Builds the insertion block for the buffered changes, appending the
    /// metadata columns for the active mode: `time`/`diff` in stream mode, or
    /// `version`/`is_deleted` in snapshot mode. In snapshot mode the version
    /// counter advances by one per buffered change, in arrival order, so a
    /// retraction is always superseded by the insertion that follows it.
    fn build_block(&mut self) -> Result<Block, WriteError> {
        let mut block = Block::new();
        for (column_id, field) in self.value_fields.iter().enumerate() {
            let column_values: Vec<&Value> = self
                .buffer
                .iter()
                .map(|context| &context.values[column_id])
                .collect();
            block = Self::append_value_column(block, &field.name, &field.type_, &column_values)?;
        }

        if self.snapshot_mode {
            let versions: Vec<u64> = (0..self.buffer.len() as u64)
                .map(|offset| self.next_version + offset)
                .collect();
            self.next_version += self.buffer.len() as u64;
            block = block.column(SNAPSHOT_FIELD_VERSION, versions);

            let is_deleted: Vec<u8> = self
                .buffer
                .iter()
                .map(|context| u8::from(context.diff < 0))
                .collect();
            block = block.column(SNAPSHOT_FIELD_IS_DELETED, is_deleted);
        } else {
            let times: Vec<i64> = self
                .buffer
                .iter()
                .map(|context| {
                    context
                        .time
                        .0
                        .try_into()
                        .expect("pathway time must fit 64-bit signed integer")
                })
                .collect();
            block = block.column(SPECIAL_FIELD_TIME, times);

            let diffs: Vec<i8> = self
                .buffer
                .iter()
                .map(|context| {
                    context
                        .diff
                        .try_into()
                        .expect("pathway diff can only be 1 or -1")
                })
                .collect();
            block = block.column(SPECIAL_FIELD_DIFF, diffs);
        }

        Ok(block)
    }
}

impl Writer for ClickHouseWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.buffer.push(data);
        if let Some(max_batch_size) = self.max_batch_size {
            if self.buffer.len() >= max_batch_size {
                self.flush(false)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let block = self.build_block()?;
        let pool = &self.pool;
        let table_name = &self.table_name;
        self.runtime.block_on(async {
            let mut client = pool.get_handle().await.map_err(ClickHouseError::Driver)?;
            client
                .insert(table_name.as_str(), block)
                .await
                .map_err(ClickHouseError::Driver)?;
            Ok::<(), WriteError>(())
        })?;
        self.buffer.clear();
        Ok(())
    }

    fn name(&self) -> String {
        format!("ClickHouse({})", self.table_name)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        // Snapshot mode keeps a global version counter, so it must run on a
        // single worker. Stream mode can be sharded across workers.
        self.snapshot_mode
    }
}

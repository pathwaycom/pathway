// Copyright © 2026 Pathway

use log::error;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::take;
use std::str::from_utf8;
use std::thread::sleep;
use std::time::Duration;

use crate::connectors::data_format::{
    create_bincoded_value, parse_bool_advanced, parse_value_from_json, serialize_value_to_json,
    FormatterContext,
};
use crate::connectors::data_storage::{
    CommitPossibility, ConversionError, SqlQueryTemplate, TableWriterInitMode, ValuesMap,
    WriteError, Writer,
};
use crate::connectors::metadata::SQLiteMetadata;
use crate::connectors::offset::EMPTY_OFFSET;
use crate::connectors::{
    DataEventType, ReadError, ReadResult, Reader, ReaderContext, StorageType, SPECIAL_FIELD_DIFF,
    SPECIAL_FIELD_TIME,
};
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::{Duration as EngineDuration, Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::python_api::ValueField;

use rusqlite::params_from_iter;
use rusqlite::types::{ToSqlOutput, Value as SqliteOwnedValue, ValueRef as SqliteValue};
use rusqlite::Connection as SqliteConnection;
use serde_json::Value as JsonValue;

const SQLITE_DATA_VERSION_PRAGMA: &str = "data_version";

/// Values of the `hidden` field returned by `PRAGMA table_xinfo` (see
/// <https://www.sqlite.org/pragma.html#pragma_table_xinfo>). `rusqlite`
/// does not expose these as named constants, so we declare them here.
/// A normal (user-visible) column has `hidden = 0`; a virtual-table
/// column declared `HIDDEN` has `hidden = 1`; the two generated
/// flavors are below.
///
/// A `GENERATED ALWAYS AS (...) VIRTUAL` column — computed on every
/// `SELECT` and not stored on disk. `SQLite` rejects `INSERT` /
/// `UPDATE` that names it.
const TABLE_XINFO_HIDDEN_GENERATED_VIRTUAL: i64 = 2;
/// A `GENERATED ALWAYS AS (...) STORED` column — computed once at
/// insert time and stored on disk. `SQLite` rejects `INSERT` /
/// `UPDATE` that names it.
const TABLE_XINFO_HIDDEN_GENERATED_STORED: i64 = 3;

/// Errors emitted by the `SQLite` reader and writer.
///
/// The `Driver` variant wraps anything coming out of `rusqlite` that
/// doesn't have a dedicated mapping; the other variants surface
/// actionable, `pw.io.sqlite`-specific failures at connector
/// construction time so users see a clear error with remediation
/// guidance instead of a silent retry loop or a runtime engine panic.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    /// Anything the `rusqlite` driver returns that isn't handled by a
    /// more specific variant (opening the database file, preparing a
    /// query the connector built, executing a statement, etc.).
    #[error("failed to perform Sqlite request: {0}")]
    Driver(#[from] ::rusqlite::Error),

    /// The reader's target object doesn't expose an implicit `_rowid_`
    /// column (typical for `WITHOUT ROWID` tables and views) and the
    /// user's schema doesn't declare a primary key to use instead.
    #[error(
        "SQLite object {0:?} does not expose an implicit `_rowid_` column \
         (typical for `WITHOUT ROWID` tables and views). Declare primary key \
         columns in your pw.Schema so the reader can identify rows."
    )]
    NoRowIdentity(String),

    /// The reader's target table has a user-defined column named
    /// `rowid`, `_rowid_`, or `oid`, which (per `SQLite`'s identifier
    /// rules) shadows the implicit integer rowid alias. With the alias
    /// shadowed the reader can't fetch the row's true integer
    /// identity, so it can't tell rows apart on subsequent polls. The
    /// user must instead declare primary-key columns in `pw.Schema`.
    #[error(
        "SQLite table {table_name:?} has a user column {shadowing_column:?} \
         that shadows SQLite's implicit rowid alias \
         (`rowid`/`_rowid_`/`oid`), so the reader can't use it for row \
         identity. Declare primary-key columns in your pw.Schema instead."
    )]
    RowIdShadowed {
        table_name: String,
        shadowing_column: String,
    },

    /// The reader's schema names one or more primary-key columns whose
    /// set does not correspond to a UNIQUE / PRIMARY KEY constraint on
    /// the target table. Without that constraint, rows sharing a key
    /// value would silently conflate in the reader's tracked snapshot.
    #[error(
        "primary_key columns {columns:?} declared in pw.Schema do not \
         correspond to any UNIQUE or PRIMARY KEY constraint on SQLite table \
         {table_name:?}. Without a matching constraint, rows that share the \
         same key value would be conflated by the reader. Either add the \
         constraint in SQLite, or change the pw.Schema to declare the \
         columns that are actually unique."
    )]
    ReaderPrimaryKeyMismatch {
        table_name: String,
        columns: Vec<String>,
    },

    /// Snapshot-mode writer with `init_mode="default"` was pointed at a
    /// pre-existing table that has no UNIQUE / PRIMARY KEY constraint
    /// on the requested `primary_key` columns. `INSERT ... ON CONFLICT (...)`
    /// cannot function without a matching constraint.
    #[error(
        "primary_key columns {columns:?} do not correspond to any UNIQUE or \
         PRIMARY KEY constraint on SQLite table {table_name:?}. Snapshot-mode \
         writes use `INSERT ... ON CONFLICT (...)` which requires a matching \
         constraint on the destination table. Add it in SQLite, or use \
         `init_mode=\"replace\"` / `init_mode=\"create_if_not_exists\"` to \
         let the writer create the table with the constraint for you."
    )]
    WriterPrimaryKeyMismatch {
        table_name: String,
        columns: Vec<String>,
    },

    /// One or more columns declared in the reader's `pw.Schema` do not
    /// exist in the target `SQLite` object. Detected at connector
    /// construction time by preflight-preparing the polling `SELECT`;
    /// failing fast here spares the user the silent-retry loop that
    /// would otherwise occur while `SQLite` re-rejects the query on
    /// every poll.
    #[error(
        "pw.Schema references column(s) {columns:?} that do not exist in \
         SQLite table {table_name:?}. Remove the column(s) from the schema, \
         rename them to match the table, or add them to the table with \
         `ALTER TABLE`."
    )]
    MissingColumn {
        table_name: String,
        columns: Vec<String>,
    },

    /// Writer with `init_mode="default"` was pointed at a table that
    /// doesn't exist. Switching the init mode to `"create_if_not_exists"`
    /// or `"replace"` lets the writer create it.
    #[error(
        "destination table {table_name:?} does not exist in the SQLite \
         database. Create it with a compatible schema, or use \
         `init_mode=\"create_if_not_exists\"` / `init_mode=\"replace\"` \
         to let the writer create it."
    )]
    DestinationTableNotFound { table_name: String },

    /// Writer was pointed at a pre-existing table that does not carry
    /// one or more columns the writer plans to `INSERT` into. In
    /// stream-of-changes mode this includes the appended `time` / `diff`
    /// metadata columns.
    #[error(
        "destination table {table_name:?} is missing required column(s) \
         {columns:?} that the writer plans to INSERT into. Add the \
         column(s) with `ALTER TABLE`, or use \
         `init_mode=\"replace\"` to let the writer recreate the table \
         with the correct schema."
    )]
    DestinationMissingColumn {
        table_name: String,
        columns: Vec<String>,
    },

    /// The path doesn't point at a valid `SQLite` 3 database — typically
    /// because the file has unrelated content, or is encrypted with a
    /// key this connection doesn't have. Detected via
    /// `PRAGMA schema_version` which returns ``SQLITE_NOTADB`` for
    /// non-database files. Genuinely empty files are NOT rejected here,
    /// because `SQLite` treats an empty file as a brand-new empty
    /// database — pointing the writer at an empty file with
    /// `init_mode="create_if_not_exists"` or `"replace"` is a supported
    /// flow. Pointing the reader at an empty file falls through to the
    /// `MissingColumn` preflight, which surfaces a clear (though
    /// table-shaped) error.
    #[error(
        "the specified file is not a valid SQLite 3 database. Verify \
         the path points to a SQLite database file (not a non-database \
         file or an encrypted database this connection can't read)."
    )]
    NotASqliteDatabase,

    /// Writer was pointed at a `SQLite` view. Views are read-only in
    /// `SQLite` — `INSERT` / `UPDATE` / `DELETE` against a view fail
    /// with `cannot modify <name> because it is a view` — unless the
    /// user has installed `INSTEAD OF` triggers that route writes to
    /// the underlying table(s).
    #[error(
        "destination {table_name:?} is a SQLite view, which SQLite treats \
         as read-only. INSERT/UPDATE/DELETE against a view are rejected \
         unless the view carries INSTEAD OF triggers that route writes \
         to the underlying table(s). Point the writer at the underlying \
         table, or attach INSTEAD OF triggers to the view first."
    )]
    DestinationIsView { table_name: String },

    /// Writer was pointed at a name that exists in `sqlite_master` but
    /// is neither a regular table nor a view — typically an index or
    /// a trigger. There's no reasonable interpretation of "writing" to
    /// such an object.
    #[error(
        "destination {table_name:?} is a SQLite {kind} in the database, \
         not a table. pw.io.sqlite.write can only target regular tables. \
         Pick a different table_name or drop the existing object first."
    )]
    DestinationIsNotATable { table_name: String, kind: String },

    /// Writer's pw.Schema names one or more columns that exist in the
    /// destination table as `GENERATED ALWAYS AS (...)` (virtual or
    /// stored). `SQLite` rejects `INSERT` / `UPDATE` that names a
    /// generated column; the user must drop the column from the
    /// schema (the reader still returns its computed value on `SELECT`).
    #[error(
        "destination table {table_name:?} has generated column(s) \
         {columns:?} (`GENERATED ALWAYS AS ...`) that pw.Schema also \
         names. SQLite does not allow INSERT/UPDATE into generated \
         columns. Remove these column(s) from the pw.Schema — the \
         reader will still return their computed values on SELECT."
    )]
    DestinationColumnIsGenerated {
        table_name: String,
        columns: Vec<String>,
    },

    /// Destination table has one or more columns declared `NOT NULL`
    /// without a `DEFAULT` that the writer's `INSERT` doesn't provide
    /// a value for. Every `INSERT` would fail with
    /// `NOT NULL constraint failed`. Safely-omittable columns
    /// (nullable, `DEFAULT`, or `INTEGER PRIMARY KEY` rowid alias)
    /// are ignored.
    #[error(
        "destination table {table_name:?} has NOT NULL column(s) \
         {columns:?} without a DEFAULT value that pw.Schema doesn't \
         provide. Every INSERT would fail with a NOT NULL constraint \
         violation. Either add these columns to pw.Schema, give them \
         a DEFAULT in SQLite, or make them nullable."
    )]
    DestinationMissingRequiredValue {
        table_name: String,
        columns: Vec<String>,
    },

    /// `pw.Schema` declares one or more columns as `Optional[T]`, but
    /// the matching destination column is `NOT NULL` and not the
    /// `INTEGER PRIMARY KEY` rowid alias. When the user's column
    /// emits `None`, the writer's `INSERT` binds `NULL` and the row
    /// fails with `NOT NULL constraint failed` at flush time. Caught
    /// at construction so the user sees the mismatch up-front instead
    /// of as a delayed engine-worker panic.
    #[error(
        "pw.Schema declares column(s) {columns:?} as Optional but the \
         destination table {table_name:?} marks them NOT NULL without \
         an auto-assigned rowid alias. A None value from Pathway would \
         fail with a NOT NULL constraint violation at flush time. \
         Either drop Optional from the Pathway column, give the SQLite \
         column a default that NULL resolves to, or make it nullable."
    )]
    DestinationColumnRequiresNonNull {
        table_name: String,
        columns: Vec<String>,
    },
}

impl From<::rusqlite::Error> for crate::connectors::ReadError {
    fn from(err: ::rusqlite::Error) -> Self {
        crate::connectors::ReadError::Sqlite(SqliteError::Driver(err))
    }
}

impl From<::rusqlite::Error> for crate::connectors::data_storage::WriteError {
    fn from(err: ::rusqlite::Error) -> Self {
        crate::connectors::data_storage::WriteError::Sqlite(SqliteError::Driver(err))
    }
}

/// Wrap a `SQLite` identifier in double quotes, escaping any embedded
/// `"` by doubling it. Lets the reader and writer accept table and
/// column names that would otherwise collide with `SQLite`'s tokenizer
/// — hyphens, spaces, reserved words, etc.
fn quote_sqlite_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Normalize the separator in a `SQLite`-stored datetime string.
///
/// `SQLite`'s `CURRENT_TIMESTAMP` default and most `datetime()`
/// function output use the SQL-92 form `YYYY-MM-DD HH:MM:SS[.sss]`
/// (space between date and time). Pathway's canonical parser (shared
/// with `pw.io.jsonlines`) expects ISO-8601 with a `T` separator.
/// This helper converts the former to the latter when the input's
/// 11th character is a space; everything else is returned unchanged
/// (including already-normalized ISO-8601 text that the Pathway
/// writer itself emits).
fn normalize_sqlite_datetime_separator(s: &str) -> String {
    if s.len() >= 11 && s.as_bytes()[10] == b' ' {
        let mut out = String::with_capacity(s.len());
        out.push_str(&s[..10]);
        out.push('T');
        out.push_str(&s[11..]);
        out
    } else {
        s.to_owned()
    }
}

/// Result of checking that the user-declared primary-key columns match
/// a UNIQUE/PRIMARY KEY constraint on a `SQLite` object.
enum PrimaryKeyCheck {
    /// The target carries a matching constraint.
    Matching,
    /// The target is a view (or other object that cannot carry
    /// constraints); the caller must treat the user's declaration as an
    /// unverifiable assertion.
    ObjectHasNoConstraints,
    /// The target is a table and carries no matching constraint.
    Mismatch,
}

/// Return `true` when `name` refers to a `SQLite` view. Reads
/// `sqlite_master.type` for an object with this name, or `None` if
/// no such object exists in the database. Regular tables have
/// `type == "table"`; other values (`"view"`, `"index"`, `"trigger"`)
/// identify non-writable targets the writer should reject up-front.
fn sqlite_object_kind(
    connection: &SqliteConnection,
    name: &str,
) -> ::rusqlite::Result<Option<String>> {
    use ::rusqlite::OptionalExtension;
    // `COLLATE NOCASE` matches SQLite's own identifier-resolution
    // rules: `PRAGMA table_info("VW")` resolves to a table named
    // `vw` case-insensitively, and the user's eventual SQL would too,
    // so the kind lookup must agree. Without `NOCASE`, the binary
    // default collation on `sqlite_master.name` lets a wrong-cased
    // `table_name` slip past every preflight and panic an engine
    // worker at flush time.
    connection
        .query_row(
            "SELECT type FROM sqlite_master WHERE name = ? COLLATE NOCASE",
            [name],
            |row| row.get::<_, String>(0),
        )
        .optional()
}

/// Preflight check: verify the connection actually points at a valid
/// `SQLite` 3 database. `SQLite`'s `open` is lazy and happily accepts any
/// file path — reading a file containing random bytes only surfaces as
/// `SQLITE_NOTADB` the first time you try to query it. Running
/// `PRAGMA schema_version` up-front lets us translate that raw driver
/// error into [`SqliteError::NotASqliteDatabase`] with actionable
/// guidance before the connector starts polling or writing. Note that
/// `SQLite` itself treats *empty* files as brand-new empty databases, so
/// this check does NOT reject them; the writer can legitimately
/// initialize one with `init_mode="create_if_not_exists"` or
/// `"replace"`, and the reader will fall through to the `MissingColumn`
/// preflight.
fn verify_is_sqlite_database(connection: &SqliteConnection) -> Result<(), SqliteError> {
    match connection.query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0)) {
        Ok(_) => Ok(()),
        Err(::rusqlite::Error::SqliteFailure(err, _))
            if err.code == ::rusqlite::ErrorCode::NotADatabase =>
        {
            Err(SqliteError::NotASqliteDatabase)
        }
        Err(e) => Err(SqliteError::Driver(e)),
    }
}

/// Return the names of schema columns that do not exist in the target
/// `SQLite` object's schema. Views, subqueries, and regular tables are
/// all handled by `PRAGMA table_info` (which returns a synthetic row
/// list for views too). Detecting missing columns up-front lets the
/// reader surface a single clear error at construction time instead of
/// letting `SQLite` reject the polling `SELECT` on every retry.
/// A single column in a `SQLite` table, classified by how the writer
/// should treat it.
struct RegularColumn {
    name: String,
    /// `true` if `NOT NULL` is set.
    notnull: bool,
    /// `true` if a `DEFAULT` clause is present.
    has_default: bool,
    /// `true` for columns that serve as the rowid alias
    /// (`INTEGER PRIMARY KEY`). `SQLite` auto-assigns a rowid when the
    /// column is omitted from `INSERT`, so it's always safely omittable
    /// regardless of `notnull`.
    is_integer_pk: bool,
}

impl RegularColumn {
    /// `true` when the writer can omit this column from its `INSERT`
    /// without tripping a constraint at flush time.
    fn is_safely_omittable(&self) -> bool {
        !self.notnull || self.has_default || self.is_integer_pk
    }
}

/// Columns of a `SQLite` object, partitioned by how the writer must
/// treat them. `generated` contains columns declared as
/// `GENERATED ALWAYS AS (...)` (virtual or stored); `SQLite` rejects
/// `INSERT`/`UPDATE` that names them, so the writer must never list
/// them in the target-column clause.
struct ColumnsLookup {
    regular: Vec<RegularColumn>,
    generated: HashSet<String>,
}

impl ColumnsLookup {
    /// Case-insensitive membership match — matches `SQLite`'s own
    /// identifier comparison rules.
    fn contains_regular(&self, name: &str) -> bool {
        let target = name.to_ascii_lowercase();
        self.regular
            .iter()
            .any(|c| c.name.to_ascii_lowercase() == target)
    }

    fn contains_generated(&self, name: &str) -> bool {
        let target = name.to_ascii_lowercase();
        self.generated
            .iter()
            .any(|n| n.to_ascii_lowercase() == target)
    }

    fn is_empty(&self) -> bool {
        self.regular.is_empty() && self.generated.is_empty()
    }
}

fn sqlite_table_columns(
    connection: &SqliteConnection,
    table_name: &str,
) -> ::rusqlite::Result<ColumnsLookup> {
    // `table_xinfo` (vs `table_info`) includes generated columns, with
    // the trailing `hidden` field distinguishing regular columns
    // (`0`) and virtual-table hidden columns (`1`, which fall through
    // into `regular` — see the fallthrough arm below) from the two
    // generated flavors that cannot be inserted into
    // (`TABLE_XINFO_HIDDEN_GENERATED_VIRTUAL` / `_STORED`).
    let mut regular = Vec::new();
    let mut generated = HashSet::new();
    let mut stmt = connection.prepare(&format!(
        "PRAGMA table_xinfo({})",
        quote_sqlite_identifier(table_name)
    ))?;
    let rows = stmt.query_map([], |row| {
        let name: String = row.get("name")?;
        let declared_type: String = row.get("type")?;
        let notnull: bool = row.get("notnull")?;
        let dflt: Option<String> = row.get("dflt_value")?;
        let pk: i64 = row.get("pk")?;
        let hidden: i64 = row.get("hidden")?;
        Ok((name, declared_type, notnull, dflt, pk, hidden))
    })?;
    // Collect rows first so we can count primary-key columns in one
    // pass; the rowid-alias check below needs that count.
    let collected: Vec<(String, String, bool, Option<String>, i64, i64)> =
        rows.collect::<::rusqlite::Result<_>>()?;
    // The rowid alias only exists when the PRIMARY KEY consists of a
    // single column — `PRAGMA table_xinfo`'s `pk` field is the
    // 1-based position *within* the PK, so `pk == 1` alone can't tell
    // a single-column PK from the first column of a composite PK.
    let pk_column_count = collected.iter().filter(|(.., pk, _)| *pk > 0).count();
    // `WITHOUT ROWID` tables do not have a rowid, so an
    // `INTEGER PRIMARY KEY` column on them is NOT the rowid alias —
    // SQLite stores the value the user provides and rejects NULL.
    // Detect this via `PRAGMA index_list`: in a `WITHOUT ROWID` table
    // SQLite reports an auto-index whose `origin == 'pk'` covers the
    // primary-key columns; a rowid table with a single-column
    // `INTEGER PRIMARY KEY` (the rowid alias) reports no such index
    // because the rowid B-tree itself serves that role. Composite or
    // non-INTEGER PKs on rowid tables also report a `pk`-origin index,
    // but those cases are independently caught by `pk_column_count`
    // and the INTEGER-name check, so keying off `pk` origin gives the
    // correct answer for the rowid-alias question on its own.
    let table_is_rowid = !connection
        .prepare(&format!(
            "PRAGMA index_list({})",
            quote_sqlite_identifier(table_name)
        ))?
        .query_map([], |row| row.get::<_, String>("origin"))?
        .any(|r| matches!(r, Ok(origin) if origin == "pk"));
    for (name, declared_type, notnull, dflt, pk, hidden) in collected {
        match hidden {
            TABLE_XINFO_HIDDEN_GENERATED_VIRTUAL | TABLE_XINFO_HIDDEN_GENERATED_STORED => {
                generated.insert(name);
            }
            // `hidden == TABLE_XINFO_HIDDEN_VIRTUAL_TABLE` (an
            // FTS5-style hidden column) falls through into `regular`
            // rather than being filtered out — the writer only
            // consults `regular` for columns that appear in the user's
            // `pw.Schema` (via `contains_regular`), so hidden columns
            // users don't reference never trip preflight checks. A
            // hidden column the user *does* reference would be
            // correctly flagged, and attempting to write to a virtual
            // table at all is out of scope for this connector.
            _ => regular.push(RegularColumn {
                name,
                notnull,
                has_default: dflt.is_some(),
                // `INTEGER PRIMARY KEY` is the rowid alias. SQLite only
                // applies this when the column is the *single-column*
                // primary key (pk == 1 and exactly one PK column in
                // the table) on a *rowid* table, and its declared type
                // resolves to INTEGER affinity — a plain "INTEGER"
                // spelling is the canonical form, and we match it
                // case-insensitively. WITHOUT ROWID tables have no
                // rowid alias even with INTEGER PRIMARY KEY, so they
                // require a value on every INSERT.
                is_integer_pk: pk == 1
                    && pk_column_count == 1
                    && table_is_rowid
                    && declared_type.eq_ignore_ascii_case("INTEGER"),
            }),
        }
    }
    Ok(ColumnsLookup { regular, generated })
}

fn columns_missing_from<'a>(
    regular: &[RegularColumn],
    required: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let regular_lower: HashSet<String> = regular
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();
    required
        .into_iter()
        .filter(|name| !regular_lower.contains(&name.to_ascii_lowercase()))
        .map(str::to_owned)
        .collect()
}

/// Check whether `table_name` has a UNIQUE or PRIMARY KEY constraint
/// whose column set equals `required_columns`. Views and other
/// index-less objects return [`PrimaryKeyCheck::ObjectHasNoConstraints`]
/// so callers can decide whether to trust the user's declaration.
///
/// Matches three kinds of constraints:
///
/// * `INTEGER PRIMARY KEY` and other `PRIMARY KEY` declarations — found
///   via `PRAGMA table_info` (the `pk` column).
/// * `PRIMARY KEY` and `UNIQUE` constraints that materialize as
///   auto-created indexes — found via `PRAGMA index_list`.
/// * Explicit `CREATE UNIQUE INDEX` — same pragma.
fn check_primary_key_match(
    connection: &SqliteConnection,
    table_name: &str,
    required_columns: &[String],
) -> ::rusqlite::Result<PrimaryKeyCheck> {
    use ::rusqlite::OptionalExtension;

    // `COLLATE NOCASE` aligns this lookup with SQLite's own
    // identifier-resolution rules — see `sqlite_object_kind` for the
    // full reasoning. Without it, a case-different `table_name` would
    // miss the view branch and incorrectly fall through to the
    // table-style PK check on a constraint-less view.
    let kind: Option<String> = connection
        .query_row(
            "SELECT type FROM sqlite_master WHERE name = ? COLLATE NOCASE",
            [table_name],
            |row| row.get(0),
        )
        .optional()?;
    if matches!(kind.as_deref(), Some("view")) {
        return Ok(PrimaryKeyCheck::ObjectHasNoConstraints);
    }

    // Column-name comparisons are ASCII-lowercased on both sides because
    // SQLite identifier matching is case-insensitive — `ID` and `id`
    // refer to the same column, and `INSERT ... ON CONFLICT (ID)` works
    // against a constraint declared on `id`. This mirrors the
    // convention already used by `ColumnsLookup::contains_regular` and
    // `check_destination_compatibility`.
    let required: HashSet<String> = required_columns
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();

    // PRAGMA table_info: PK columns carry a `pk` position > 0. Also
    // catches `INTEGER PRIMARY KEY`, which does NOT create an index.
    let pk_from_table_info: HashSet<String> = connection
        .prepare(&format!(
            "PRAGMA table_info({})",
            quote_sqlite_identifier(table_name)
        ))?
        .query_map([], |row| {
            let name: String = row.get("name")?;
            let pk_pos: i64 = row.get("pk")?;
            Ok((name, pk_pos))
        })?
        .filter_map(|r| match r {
            Ok((name, pk)) if pk > 0 => Some(Ok(name.to_ascii_lowercase())),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<::rusqlite::Result<_>>()?;
    if pk_from_table_info == required {
        return Ok(PrimaryKeyCheck::Matching);
    }

    // PRAGMA index_list → indexes. Keep UNIQUE, non-partial ones, then
    // check column names via PRAGMA index_info. Partial indexes
    // (`CREATE UNIQUE INDEX ... WHERE ...`) don't satisfy
    // `ON CONFLICT` — they only enforce uniqueness over a subset of
    // rows, so duplicates are allowed where the WHERE clause is false
    // — and SQLite refuses to use them as the conflict target.
    let unique_indexes: Vec<String> = connection
        .prepare(&format!(
            "PRAGMA index_list({})",
            quote_sqlite_identifier(table_name)
        ))?
        .query_map([], |row| {
            let name: String = row.get("name")?;
            let is_unique: bool = row.get("unique")?;
            let is_partial: bool = row.get("partial")?;
            Ok((name, is_unique, is_partial))
        })?
        .filter_map(|r| match r {
            Ok((name, true, false)) => Some(Ok(name)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<::rusqlite::Result<_>>()?;
    for index_name in unique_indexes {
        // Functional / expression-based indexes (e.g.
        // `CREATE UNIQUE INDEX idx ON t(lower(v))`) report `name = NULL`
        // for the expression entries. Skip those; for matching
        // purposes we only care about bare-column entries. A purely
        // functional index collapses to an empty column set and will
        // never match a non-empty required set — which is the right
        // semantics, since SQLite won't accept `ON CONFLICT (col)`
        // against an expression-only index anyway.
        let index_cols: HashSet<String> = connection
            .prepare(&format!(
                "PRAGMA index_info({})",
                quote_sqlite_identifier(&index_name)
            ))?
            .query_map([], |row| row.get::<_, Option<String>>("name"))?
            .filter_map(|r| match r {
                Ok(Some(name)) => Some(Ok(name.to_ascii_lowercase())),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<::rusqlite::Result<_>>()?;
        if index_cols == required {
            return Ok(PrimaryKeyCheck::Matching);
        }
    }

    Ok(PrimaryKeyCheck::Mismatch)
}

/// How a single row is identified across polls, so the reader can diff
/// the current state against `stored_state` and emit Insert/Delete
/// events.
#[derive(Debug)]
enum RowIdentity {
    /// Use primary-key columns declared in the user's `pw.Schema`. The
    /// indices are positions within `schema` — stored once at
    /// construction time to avoid name lookups on every row.
    SchemaPrimaryKey { positions: Vec<usize> },
    /// Use `SQLite`'s implicit `_rowid_` column. Selected as an extra
    /// column alongside the schema columns in the polling query.
    /// Unavailable for `WITHOUT ROWID` tables and views.
    Rowid,
}

pub struct SqliteReader {
    connection: SqliteConnection,
    table_name: String,
    schema: Vec<(String, Type)>,
    row_identity: RowIdentity,

    last_saved_data_version: Option<i64>,
    stored_state: HashMap<Vec<Value>, ValuesMap>,
    queued_updates: VecDeque<ReadResult>,
}

impl SqliteReader {
    /// Construct a new reader.
    ///
    /// `key_field_names`, when supplied, names columns from the user's
    /// schema that together identify a row — those columns' values are
    /// tracked instead of the `_rowid_`. When omitted, the reader falls
    /// back to `SQLite`'s implicit `_rowid_`; that in turn requires the
    /// target `SQLite` object to actually expose it (regular tables do,
    /// `WITHOUT ROWID` tables and views don't). If neither a schema
    /// primary key nor `_rowid_` is available, `new` fails with
    /// [`SqliteError::NoRowIdentity`].
    pub fn new(
        connection: SqliteConnection,
        table_name: String,
        schema: Vec<(String, Type)>,
        key_field_names: Option<Vec<String>>,
    ) -> Result<Self, ReadError> {
        // Preflight: confirm the file is actually a SQLite database —
        // `open` is lazy, so an empty or corrupted file only trips at
        // the first query. Failing here gives a Pathway-level message
        // instead of letting SQLITE_NOTADB propagate through the
        // retry loop.
        verify_is_sqlite_database(&connection)?;

        // Preflight: every column named in `schema` must exist in the
        // target `SQLite` object. If it doesn't, the polling `SELECT`
        // would fail at prepare time on every poll and the reader's
        // retry-on-error loop would never surface the cause. Fail fast
        // with the names of the offending columns instead.
        // The reader's polling SELECT reads every column in the schema,
        // including generated ones (SQLite computes those on the fly),
        // so "missing" from the reader's perspective means "neither a
        // regular nor a generated column on the target".
        let existing = sqlite_table_columns(&connection, &table_name)?;
        let missing: Vec<String> = schema
            .iter()
            .map(|(n, _)| n.clone())
            .filter(|n| !existing.contains_regular(n) && !existing.contains_generated(n))
            .collect();
        if !missing.is_empty() {
            return Err(SqliteError::MissingColumn {
                table_name,
                columns: missing,
            }
            .into());
        }

        let row_identity = match key_field_names {
            Some(names) if !names.is_empty() => {
                // Guard: the user-declared primary key must match a
                // UNIQUE/PRIMARY KEY constraint in SQLite, otherwise the
                // reader would silently conflate rows sharing the same
                // key value into one `stored_state` entry and churn
                // Delete/Insert events on every poll. Views are allowed
                // through because they carry no constraints and the
                // user is asserting uniqueness.
                match check_primary_key_match(&connection, &table_name, &names)? {
                    PrimaryKeyCheck::Matching | PrimaryKeyCheck::ObjectHasNoConstraints => {}
                    PrimaryKeyCheck::Mismatch => {
                        return Err(SqliteError::ReaderPrimaryKeyMismatch {
                            table_name,
                            columns: names,
                        }
                        .into());
                    }
                }

                // Resolve each PK name to its position in `schema`. If
                // any name is missing, that's a Pathway-internal mismatch
                // between the schema PK declaration and the value
                // fields, not a user error — surface it by panicking
                // (the Python layer should never allow this).
                let positions = names
                    .iter()
                    .map(|name| {
                        schema
                            .iter()
                            .position(|(col_name, _)| col_name == name)
                            .unwrap_or_else(|| {
                                panic!(
                                    "primary key column {name:?} is not present \
                                     in the SQLite reader's schema"
                                )
                            })
                    })
                    .collect();
                RowIdentity::SchemaPrimaryKey { positions }
            }
            _ => {
                // Preflight: if `_rowid_` isn't selectable, fail fast
                // with a targeted error rather than letting the polling
                // loop emit identical SQL errors forever.
                let probe = format!(
                    "SELECT _rowid_ FROM {} LIMIT 0",
                    quote_sqlite_identifier(&table_name)
                );
                if connection.prepare(&probe).is_err() {
                    return Err(SqliteError::NoRowIdentity(table_name).into());
                }
                // Even when `SELECT _rowid_` prepares successfully, a
                // user-defined column named `rowid`/`_rowid_`/`oid` (per
                // SQLite's identifier rules these names are
                // case-insensitive) shadows the implicit integer rowid
                // alias — `SELECT _rowid_` then returns the user
                // column's storage class instead of the integer rowid,
                // and every subsequent `row.get::<i64>(...)` errors out
                // forever. Detect the shadow up-front and force the user
                // to declare a schema-level primary key.
                if let Some(shadow) = existing.regular.iter().find(|c| {
                    let lower = c.name.to_ascii_lowercase();
                    lower == "rowid" || lower == "_rowid_" || lower == "oid"
                }) {
                    return Err(SqliteError::RowIdShadowed {
                        table_name,
                        shadowing_column: shadow.name.clone(),
                    }
                    .into());
                }
                RowIdentity::Rowid
            }
        };

        Ok(Self {
            connection,
            table_name,
            schema,
            row_identity,

            last_saved_data_version: None,
            queued_updates: VecDeque::new(),
            stored_state: HashMap::new(),
        })
    }

    /// Data version is required to check if there was an update in the database.
    /// There are also hooks, but they only work for changes happened in the same
    /// connection.
    /// More details why hooks don't help here: <https://sqlite.org/forum/forumpost/3174b39eeb79b6a4>
    pub fn data_version(&self) -> i64 {
        loop {
            let version: ::rusqlite::Result<i64> = self.connection.pragma_query_value(
                Some(::rusqlite::DatabaseName::Main),
                SQLITE_DATA_VERSION_PRAGMA,
                |row| row.get(0),
            );
            match version {
                Ok(version) => return version,
                Err(e) => error!("pragma.data_version request has failed: {e}"),
            }
            sleep(Self::retry_after_error_period());
        }
    }

    /// Convert raw `SQLite` field into one of internal value types.
    ///
    /// `SQLite` natively supports only five storage classes: null, integer,
    /// real, text, and blob (see <https://www.sqlite.org/datatype3.html>).
    /// Pathway `Value` has many more variants, so types that lack a native
    /// mapping are expected to be stored using the same representation that
    /// `pw.io.jsonlines` would produce:
    ///
    /// * `Bool` — `INTEGER` 0/1 (standard `SQLite` convention);
    /// * `Duration` — `INTEGER` number of nanoseconds;
    /// * `DateTimeNaive` — `TEXT` formatted as `%Y-%m-%dT%H:%M:%S%.f`;
    /// * `DateTimeUtc` — `TEXT` formatted as `%Y-%m-%dT%H:%M:%S%.f%z`;
    /// * `Pointer` — `TEXT` with the Pathway base32 pointer encoding;
    /// * `PyObjectWrapper` — `TEXT` with the base64-encoded bincode payload;
    /// * `Json` — `TEXT` containing a JSON document;
    /// * `Tuple`, `List`, `Array` — `TEXT` containing the same JSON
    ///   structure that the JSON-lines formatter emits (arrays for tuples
    ///   and lists, `{"shape": [...], "elements": [...]}` for n-d arrays).
    fn convert_to_value(
        orig_value: SqliteValue<'_>,
        field_name: &str,
        dtype: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        Self::parse_sqlite_value(orig_value, dtype).ok_or_else(|| {
            let value_repr = limit_length(format!("{orig_value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
            Box::new(ConversionError::new(
                value_repr,
                field_name.to_owned(),
                dtype.clone(),
                None,
            ))
        })
    }

    fn parse_sqlite_value(orig_value: SqliteValue<'_>, dtype: &Type) -> Option<Value> {
        // `ValueRef::as_{i64,f64,str,blob}` return an error for any storage
        // class that does not match the requested one (including NULL), so
        // `.ok()` collapses every mismatched variant into `None` without
        // forcing us to respell the rejection pattern at every call site.
        match dtype {
            // Optional: NULL becomes Value::None, any other value is parsed
            // against the inner type.
            Type::Optional(arg) => match orig_value {
                SqliteValue::Null => Some(Value::None),
                other => Self::parse_sqlite_value(other, arg),
            },

            // Any accepts every SQLite storage class via its most natural mapping.
            Type::Any => match orig_value {
                SqliteValue::Null => Some(Value::None),
                SqliteValue::Integer(val) => Some(Value::Int(val)),
                SqliteValue::Real(val) => Some(Value::Float(val.into())),
                SqliteValue::Text(val) => from_utf8(val)
                    .ok()
                    .map(|parsed_string| Value::String(parsed_string.into())),
                SqliteValue::Blob(val) => Some(Value::Bytes(val.into())),
            },

            // Primitive types backed by a single SQLite storage class.
            Type::Int => orig_value.as_i64().ok().map(Value::Int),
            // `Float` accepts both `REAL` and `INTEGER` (SQLite has no
            // concept of a numeric-column affinity, so integer-valued
            // literals routinely land in REAL columns; widening an i64
            // to f64 mirrors how `SQLite` itself coerces between the two
            // numeric storage classes).
            Type::Float => orig_value
                .as_f64()
                .ok()
                .or_else(|| {
                    orig_value.as_i64().ok().map(|i| {
                        #[allow(clippy::cast_precision_loss)]
                        {
                            i as f64
                        }
                    })
                })
                .map(|f| Value::Float(f.into())),
            Type::String => orig_value
                .as_str()
                .ok()
                .map(|parsed_string| Value::String(parsed_string.into())),
            Type::Bytes => orig_value.as_blob().ok().map(|b| Value::Bytes(b.into())),

            // Bool is accepted in two forms: as INTEGER 0/1 (the usual
            // SQLite convention) or as a PostgreSQL-style textual literal
            // (`true`/`false`, `yes`/`no`, `on`/`off`, `1`/`0`, `t`/`f`,
            // `y`/`n`; case-insensitive, whitespace-trimmed).
            Type::Bool => orig_value
                .as_i64()
                .ok()
                .and_then(|val| match val {
                    0 => Some(false),
                    1 => Some(true),
                    _ => None,
                })
                .or_else(|| {
                    orig_value
                        .as_str()
                        .ok()
                        .and_then(|s| parse_bool_advanced(s).ok())
                })
                .map(Value::Bool),

            // Duration is stored as INTEGER nanoseconds (matches jsonlines).
            Type::Duration => orig_value
                .as_i64()
                .ok()
                .and_then(|val| EngineDuration::new_with_unit(val, "ns").ok())
                .map(Value::Duration),

            // JSON document stored verbatim in a TEXT column.
            Type::Json => orig_value
                .as_str()
                .ok()
                .and_then(|parsed_string| serde_json::from_str::<JsonValue>(parsed_string).ok())
                .map(Value::from),

            // Pointer and PyObjectWrapper use bespoke string encodings
            // (base32 / base64-bincode); they round-trip verbatim
            // through the jsonlines parser.
            Type::Pointer | Type::PyObjectWrapper => {
                orig_value.as_str().ok().and_then(|parsed_string| {
                    parse_value_from_json(&JsonValue::String(parsed_string.to_owned()), dtype)
                })
            }

            // Datetimes: the jsonlines parser expects ISO-8601 with a
            // `T` separator (what our writer emits), but `SQLite`
            // users commonly store the SQL-92 form with a space —
            // that's what `CURRENT_TIMESTAMP` produces. Normalize the
            // separator before handing off so both forms round-trip.
            Type::DateTimeNaive | Type::DateTimeUtc => orig_value.as_str().ok().and_then(|s| {
                let normalized = normalize_sqlite_datetime_separator(s);
                parse_value_from_json(&JsonValue::String(normalized), dtype)
            }),

            // Complex types whose jsonlines representation is a JSON array or
            // object — the TEXT column is expected to hold that JSON.
            Type::Tuple(_) | Type::List(_) | Type::Array(_, _) => orig_value
                .as_str()
                .ok()
                .and_then(|parsed_string| serde_json::from_str::<JsonValue>(parsed_string).ok())
                .and_then(|json_value| parse_value_from_json(&json_value, dtype)),

            // Future is a type-system placeholder for async results; it is
            // unwrapped before rows reach any connector, so it should not
            // appear here. Fail explicitly rather than silently accepting.
            Type::Future(_) => None,
        }
    }

    fn polling_query(&self) -> String {
        let column_list = self
            .schema
            .iter()
            .map(|(name, _dtype)| quote_sqlite_identifier(name))
            .collect::<Vec<_>>()
            .join(",");
        // In `Rowid` mode we select `_rowid_` as an extra trailing
        // column; in `SchemaPrimaryKey` mode the identity is already
        // inside the schema columns we're about to read.
        match self.row_identity {
            RowIdentity::Rowid => format!(
                "SELECT {column_list},_rowid_ FROM {}",
                quote_sqlite_identifier(&self.table_name)
            ),
            RowIdentity::SchemaPrimaryKey { .. } => format!(
                "SELECT {column_list} FROM {}",
                quote_sqlite_identifier(&self.table_name)
            ),
        }
    }

    /// Extract the row-identity tuple from a single row, given the
    /// already-parsed `values` map for its schema columns.
    ///
    /// In `SchemaPrimaryKey` mode, primary-key columns that failed to
    /// parse are identified by `Value::Error` so later polls with the
    /// same underlying bytes map to the same identity — otherwise we'd
    /// double-insert or double-delete the broken row every poll.
    fn extract_key(
        &self,
        row: &rusqlite::Row<'_>,
        values: &HashMap<String, Result<Value, Box<ConversionError>>>,
    ) -> Result<Vec<Value>, ReadError> {
        match &self.row_identity {
            RowIdentity::Rowid => {
                let rowid: i64 = row.get(self.schema.len())?;
                Ok(vec![Value::Int(rowid)])
            }
            RowIdentity::SchemaPrimaryKey { positions } => Ok(positions
                .iter()
                .map(|&pos| {
                    let name = &self.schema[pos].0;
                    values
                        .get(name)
                        .expect("schema column must be in values")
                        .clone()
                        .unwrap_or(Value::Error)
                })
                .collect()),
        }
    }

    /// Build a per-row error event for a row whose primary-key identity
    /// duplicates another row already seen in the same poll.
    ///
    /// `SQLite` allows multiple `NULL`s in a `UNIQUE` column (and in the
    /// legacy non-strict `PRIMARY KEY` form on rowid tables), so
    /// well-formed SQL can still produce repeated identities from the
    /// reader's point of view. Rather than silently coalescing those
    /// rows in `stored_state` (where they would churn Delete/Insert
    /// pairs on every poll), we forward each duplicate downstream as an
    /// error-bearing event: each primary-key column is replaced with a
    /// `ConversionError`, the engine assigns a synthetic identity
    /// (`key = None`), and the row does NOT enter `stored_state` —
    /// meaning the rest of the batch still lands in the table as
    /// expected.
    fn duplicate_key_error_event(
        &self,
        key: &[Value],
        mut values: HashMap<String, Result<Value, Box<ConversionError>>>,
    ) -> ReadResult {
        if let RowIdentity::SchemaPrimaryKey { positions } = &self.row_identity {
            let value_repr = limit_length(
                key.iter()
                    .map(|v| format!("{v}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                STANDARD_OBJECT_LENGTH_LIMIT,
            );
            let detail = format!(
                "duplicate primary key [{value_repr}] in SQLite table {:?}: \
                 multiple rows share this identity (exactly one NULL is allowed \
                 per UNIQUE column in SQLite; further NULLs, or any non-unique \
                 repeats, collide in the reader's identity tracking)",
                self.table_name,
            );
            for &pos in positions {
                let (name, type_) = &self.schema[pos];
                values.insert(
                    name.clone(),
                    Err(Box::new(ConversionError::new(
                        value_repr.clone(),
                        name.clone(),
                        type_.clone(),
                        Some(detail.clone()),
                    ))),
                );
            }
        }
        let values: ValuesMap = values.into();
        ReadResult::Data(
            ReaderContext::from_diff(DataEventType::Insert, None, values),
            EMPTY_OFFSET,
        )
    }

    fn load_table(&mut self) -> Result<(), ReadError> {
        let query = self.polling_query();
        let mut statement = self.connection.prepare(&query)?;
        let mut rows = statement.query([])?;

        let mut present_keys: HashSet<Vec<Value>> = HashSet::new();
        let mut duplicate_events: Vec<ReadResult> = Vec::new();
        while let Some(row) = rows.next()? {
            let mut values = HashMap::with_capacity(self.schema.len());
            for (column_idx, (column_name, column_dtype)) in self.schema.iter().enumerate() {
                let value =
                    Self::convert_to_value(row.get_ref(column_idx)?, column_name, column_dtype);
                values.insert(column_name.clone(), value);
            }
            let key = self.extract_key(row, &values)?;

            // Duplicates (including multiple NULLs that share a key in
            // `SchemaPrimaryKey` mode) are forwarded as per-row errors;
            // the rest of the batch is processed normally.
            if present_keys.contains(&key) {
                duplicate_events.push(self.duplicate_key_error_event(&key, values));
                continue;
            }

            let values: ValuesMap = values.into();
            self.stored_state
                .entry(key.clone())
                .and_modify(|current_values| {
                    if current_values != &values {
                        self.queued_updates.push_back(ReadResult::Data(
                            ReaderContext::from_diff(
                                DataEventType::Delete,
                                Some(key.clone()),
                                take(current_values),
                            ),
                            EMPTY_OFFSET,
                        ));
                        self.queued_updates.push_back(ReadResult::Data(
                            ReaderContext::from_diff(
                                DataEventType::Insert,
                                Some(key.clone()),
                                values.clone(),
                            ),
                            EMPTY_OFFSET,
                        ));
                        current_values.clone_from(&values);
                    }
                })
                .or_insert_with(|| {
                    self.queued_updates.push_back(ReadResult::Data(
                        ReaderContext::from_diff(
                            DataEventType::Insert,
                            Some(key.clone()),
                            values.clone(),
                        ),
                        EMPTY_OFFSET,
                    ));
                    values
                });
            present_keys.insert(key);
        }
        for event in duplicate_events {
            self.queued_updates.push_back(event);
        }

        self.stored_state.retain(|key, values| {
            if present_keys.contains(key) {
                true
            } else {
                self.queued_updates.push_back(ReadResult::Data(
                    ReaderContext::from_diff(
                        DataEventType::Delete,
                        Some(key.clone()),
                        take(values),
                    ),
                    EMPTY_OFFSET,
                ));
                false
            }
        });

        if !self.queued_updates.is_empty() {
            self.queued_updates.push_back(ReadResult::FinishedSource {
                commit_possibility: CommitPossibility::Possible,
            });
        }

        Ok(())
    }

    fn wait_period() -> Duration {
        Duration::from_millis(500)
    }

    fn retry_after_error_period() -> Duration {
        Duration::from_millis(500)
    }
}

impl Reader for SqliteReader {
    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        // SQLite has no change-log history to replay, so persistent
        // resumption of `pw.io.sqlite.read` is unsupported. Surface a
        // targeted error instead of `todo!()` panicking an engine
        // worker on the initial `seek` call. This mirrors how
        // `PsqlReader` handles the same situation.
        Err(ReadError::PersistenceNotSupported(StorageType::Sqlite))
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(queued_update) = self.queued_updates.pop_front() {
                return Ok(queued_update);
            }

            let current_data_version = self.data_version();
            if self.last_saved_data_version != Some(current_data_version) {
                self.load_table()?;
                self.last_saved_data_version = Some(current_data_version);
                return Ok(ReadResult::NewSource(
                    SQLiteMetadata::new(current_data_version).into(),
                ));
            }
            // Sleep to avoid non-stop pragma requests of a table
            // that did not change
            sleep(Self::wait_period());
        }
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("SQLite({})", self.table_name).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Sqlite
    }
}

/// Writer counterpart to [`SqliteReader`].
///
/// Values are serialized using the same storage layout that
/// [`SqliteReader`] parses — primitives land in their native `SQLite`
/// storage classes and composite values are stored as the JSON text
/// produced by the JSON-lines formatter — so a write/read cycle round-trips
/// without loss.
///
/// Two modes mirror the other SQL output connectors
/// ([`PsqlWriter`](super::postgres::PsqlWriter), [`MysqlWriter`](super::MysqlWriter)):
///
/// * **Stream of changes** (default): every event is appended to the
///   destination table together with its `time` / `diff` metadata; the
///   result is a full log of insertions *and* deletions.
/// * **Snapshot**: the destination table holds the current state. Insertions
///   are emitted as `INSERT ... ON CONFLICT (pk) DO UPDATE SET col=excluded.col`
///   (UPSERT), and deletions as `DELETE ... WHERE pk=?`. A primary key is
///   required.
pub struct SqliteWriter {
    connection: SqliteConnection,
    table_name: String,
    query_template: SqlQueryTemplate,
    snapshot_mode: bool,
    buffer: Vec<FormatterContext>,
    max_batch_size: Option<usize>,
}

impl SqliteWriter {
    pub fn new(
        connection: SqliteConnection,
        table_name: String,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        snapshot_mode: bool,
        init_mode: TableWriterInitMode,
        max_batch_size: Option<usize>,
    ) -> Result<Self, WriteError> {
        // The shared SQL builders (`TableWriterInitMode::initialize` and
        // `SqlQueryTemplate::new`) interpolate identifiers unquoted, which
        // is the right thing for dialects that use backticks / square
        // brackets. For `SQLite` we pre-quote the table and column names
        // at the boundary so downstream string-concatenation stays oblivious
        // and names with hyphens, spaces, or reserved words still produce
        // valid SQL.
        // Preflight: confirm the file is actually a SQLite database
        // before any DDL or DML runs. `open` is lazy and will accept
        // anything; without this check the raw SQLITE_NOTADB error
        // surfaces as a Rust-worker panic at flush time.
        verify_is_sqlite_database(&connection)?;

        // Preflight: SQLite views are read-only. Catch the case up-front
        // rather than letting `cannot modify <name> because it is a view`
        // panic an engine worker at flush time.
        match sqlite_object_kind(&connection, &table_name)?.as_deref() {
            // Regular table or nothing with that name yet — writer's
            // subsequent init_mode / destination checks deal with it.
            Some("table") | None => {}
            Some("view") => {
                return Err(SqliteError::DestinationIsView { table_name }.into());
            }
            Some(kind) => {
                return Err(SqliteError::DestinationIsNotATable {
                    table_name,
                    kind: kind.to_owned(),
                }
                .into());
            }
        }

        let quoted_table_name = quote_sqlite_identifier(&table_name);
        let quoted_value_fields: Vec<ValueField> = value_fields
            .iter()
            .map(|f| {
                let mut cloned = f.clone();
                cloned.name = quote_sqlite_identifier(&f.name);
                cloned
            })
            .collect();
        let quoted_key_field_names: Option<Vec<String>> = key_field_names.map(|names| {
            names
                .iter()
                .map(|name| quote_sqlite_identifier(name))
                .collect()
        });

        // For snapshot writes against a pre-existing table, verify the
        // destination carries a UNIQUE/PRIMARY KEY constraint that matches
        // `primary_key`. Without it the `INSERT ... ON CONFLICT (...)` we
        // emit per row would fail mid-flush with SQLite's opaque "ON
        // CONFLICT clause does not match any PRIMARY KEY or UNIQUE
        // constraint" error. The check fires for both `Default` and
        // `CreateIfNotExists` because both can land on a pre-existing
        // table the writer didn't shape itself — `CreateIfNotExists`'
        // CREATE is a no-op when the table is already there, so a
        // user-maintained table without the right constraint slips
        // straight through into the SQL template. `Replace` is exempt
        // because the subsequent DROP+CREATE always installs the
        // correct constraint. We also skip the check when the table
        // doesn't exist yet — `CreateIfNotExists` will then build it
        // with a matching PRIMARY KEY clause, and `Default` would fail
        // the later `DestinationTableNotFound` preflight with a more
        // actionable error.
        let may_hit_preexisting_table = matches!(
            init_mode,
            TableWriterInitMode::Default | TableWriterInitMode::CreateIfNotExists,
        );
        let table_already_exists = sqlite_object_kind(&connection, &table_name)?.is_some();
        if snapshot_mode && may_hit_preexisting_table && table_already_exists {
            if let Some(names) = key_field_names {
                match check_primary_key_match(&connection, &table_name, names)? {
                    PrimaryKeyCheck::Matching | PrimaryKeyCheck::ObjectHasNoConstraints => {}
                    PrimaryKeyCheck::Mismatch => {
                        return Err(SqliteError::WriterPrimaryKeyMismatch {
                            table_name,
                            columns: names.to_vec(),
                        }
                        .into());
                    }
                }
            }
        }

        init_mode.initialize(
            &quoted_table_name,
            &quoted_value_fields,
            quoted_key_field_names.as_deref(),
            !snapshot_mode,
            |query| {
                connection.execute(query, [])?;
                Ok(())
            },
            Self::sqlite_data_type,
        )?;

        // After init_mode has had its say, verify the destination table
        // really exists and carries every column the writer plans to
        // INSERT into. `Default` never touches the DB, and
        // `CreateIfNotExists` is a no-op when the table is already
        // there — either way, a pre-existing table with a different
        // shape would only surface as a driver-level panic at flush
        // time. `Replace` always just created the table so the check
        // is also safe there (it'll trivially pass).
        Self::check_destination_compatibility(
            &connection,
            &table_name,
            value_fields,
            snapshot_mode,
        )?;

        let query_template = SqlQueryTemplate::new(
            snapshot_mode,
            &quoted_table_name,
            &quoted_value_fields,
            quoted_key_field_names.as_deref(),
            false,
            |_| "?".to_string(),
            Self::on_insert_conflict_condition,
        )?;

        Ok(Self {
            connection,
            table_name,
            query_template,
            snapshot_mode,
            buffer: Vec::new(),
            max_batch_size,
        })
    }

    /// Verify the destination table exists and is shape-compatible
    /// with what the writer will `INSERT` into. Surfaces Pathway-level
    /// errors for the four common misconfigurations (missing table,
    /// missing / generated / extra-NOT-NULL columns, and Optional
    /// pw.Schema columns landing on NOT NULL destination columns) so
    /// users don't hit driver-level panics at flush time.
    fn check_destination_compatibility(
        connection: &SqliteConnection,
        table_name: &str,
        value_fields: &[ValueField],
        snapshot_mode: bool,
    ) -> Result<(), WriteError> {
        let existing_columns = sqlite_table_columns(connection, table_name)?;
        if existing_columns.is_empty() {
            return Err(SqliteError::DestinationTableNotFound {
                table_name: table_name.to_owned(),
            }
            .into());
        }

        let mut required_columns: Vec<&str> =
            value_fields.iter().map(|f| f.name.as_str()).collect();
        if !snapshot_mode {
            // Stream-of-changes mode appends `time` / `diff` columns
            // to every INSERT; the destination must carry them too.
            required_columns.push(SPECIAL_FIELD_TIME);
            required_columns.push(SPECIAL_FIELD_DIFF);
        }

        // Required column landing on a GENERATED column — SQLite won't
        // let us INSERT into it, so error with the situation named
        // correctly rather than claiming the column is absent.
        let generated: Vec<String> = required_columns
            .iter()
            .filter(|n| existing_columns.contains_generated(n))
            .map(|n| (*n).to_owned())
            .collect();
        if !generated.is_empty() {
            return Err(SqliteError::DestinationColumnIsGenerated {
                table_name: table_name.to_owned(),
                columns: generated,
            }
            .into());
        }

        let missing =
            columns_missing_from(&existing_columns.regular, required_columns.iter().copied());
        if !missing.is_empty() {
            return Err(SqliteError::DestinationMissingColumn {
                table_name: table_name.to_owned(),
                columns: missing,
            }
            .into());
        }

        // Every extra column in the destination (not in our INSERT's
        // column list) must be safely omittable: nullable, carries a
        // DEFAULT clause, or is the INTEGER PRIMARY KEY rowid alias.
        // Otherwise every INSERT would trip a NOT NULL constraint.
        let required_lower: HashSet<String> = required_columns
            .iter()
            .map(|n| n.to_ascii_lowercase())
            .collect();
        let requires_value: Vec<String> = existing_columns
            .regular
            .iter()
            .filter(|c| !required_lower.contains(&c.name.to_ascii_lowercase()))
            .filter(|c| !c.is_safely_omittable())
            .map(|c| c.name.clone())
            .collect();
        if !requires_value.is_empty() {
            return Err(SqliteError::DestinationMissingRequiredValue {
                table_name: table_name.to_owned(),
                columns: requires_value,
            }
            .into());
        }

        // Nullability mismatch: a pw.Schema column declared Optional
        // that lands on a NOT NULL destination column (and isn't the
        // INTEGER PRIMARY KEY rowid alias, which SQLite auto-assigns
        // for NULL). Any None value from Pathway would then hit a
        // NOT NULL constraint violation at flush time. Surface this
        // at construction so the mismatch is visible to the user
        // up-front, consistent with how the other preflight cases
        // behave. Matching is done case-insensitively (SQLite's
        // identifier rules). Note that we do NOT treat `has_default`
        // as an escape hatch here: the writer always binds a value
        // (including NULL), so a DEFAULT clause is bypassed at INSERT
        // time and doesn't rescue a NULL bind.
        let column_by_name: HashMap<String, &RegularColumn> = existing_columns
            .regular
            .iter()
            .map(|c| (c.name.to_ascii_lowercase(), c))
            .collect();
        let optional_on_not_null: Vec<String> = value_fields
            .iter()
            .filter(|f| matches!(f.type_, Type::Optional(_)))
            .filter_map(|f| {
                column_by_name
                    .get(&f.name.to_ascii_lowercase())
                    .filter(|c| c.notnull && !c.is_integer_pk)
                    .map(|c| c.name.clone())
            })
            .collect();
        if !optional_on_not_null.is_empty() {
            return Err(SqliteError::DestinationColumnRequiresNonNull {
                table_name: table_name.to_owned(),
                columns: optional_on_not_null,
            }
            .into());
        }

        Ok(())
    }

    /// Generates the `ON CONFLICT` suffix appended to the `INSERT` in
    /// snapshot mode. Uses `SQLite`'s `excluded.<col>` pseudo-table syntax
    /// (supported since 3.24) so we don't need to rebind the same values
    /// a second time for the update branch.
    fn on_insert_conflict_condition(
        _table_name: &str,
        key_field_names: &[String],
        value_fields: &[ValueField],
        _include_special_columns: bool,
    ) -> String {
        let key_set: HashSet<&str> = key_field_names.iter().map(String::as_str).collect();
        let update_pairs: Vec<String> = value_fields
            .iter()
            .filter(|f| !key_set.contains(f.name.as_str()))
            .map(|f| format!("{name}=excluded.{name}", name = f.name))
            .collect();
        if update_pairs.is_empty() {
            format!("ON CONFLICT ({}) DO NOTHING", key_field_names.join(","))
        } else {
            format!(
                "ON CONFLICT ({}) DO UPDATE SET {}",
                key_field_names.join(","),
                update_pairs.join(", ")
            )
        }
    }

    /// Map a Pathway type onto the `SQLite` storage class that the writer
    /// produces (and the reader accepts). See the module-level mapping
    /// table and the reader's `parse_sqlite_value` for the inverse.
    fn sqlite_data_type(type_: &Type, is_nested: bool) -> Result<String, WriteError> {
        let not_null_suffix = if is_nested { "" } else { " NOT NULL" };
        let base = match type_ {
            Type::Optional(wrapped) => return Self::sqlite_data_type(wrapped, true),
            Type::Bool | Type::Int | Type::Duration => "INTEGER",
            Type::Float => "REAL",
            Type::Bytes => "BLOB",
            Type::String
            | Type::Pointer
            | Type::DateTimeNaive
            | Type::DateTimeUtc
            | Type::Json
            | Type::Tuple(_)
            | Type::List(_)
            | Type::Array(_, _)
            | Type::PyObjectWrapper => "TEXT",
            Type::Any | Type::Future(_) => return Err(WriteError::UnsupportedType(type_.clone())),
        };
        Ok(format!("{base}{not_null_suffix}"))
    }

    /// Encode a Pathway [`Value`] in the `SQLite` storage classes that
    /// [`SqliteReader::parse_sqlite_value`] accepts.
    fn value_to_sqlite(value: &Value) -> Result<SqliteOwnedValue, WriteError> {
        Ok(match value {
            Value::None => SqliteOwnedValue::Null,
            Value::Bool(b) => SqliteOwnedValue::Integer(i64::from(*b)),
            Value::Int(i) => SqliteOwnedValue::Integer(*i),
            Value::Float(f) => SqliteOwnedValue::Real(f.into_inner()),
            Value::String(s) => SqliteOwnedValue::Text(s.to_string()),
            Value::Bytes(b) => SqliteOwnedValue::Blob(b.to_vec()),
            Value::Pointer(k) => SqliteOwnedValue::Text(k.to_string()),
            Value::DateTimeNaive(dt) => SqliteOwnedValue::Text(dt.to_string()),
            Value::DateTimeUtc(dt) => SqliteOwnedValue::Text(dt.to_string()),
            Value::Duration(d) => SqliteOwnedValue::Integer(d.nanoseconds()),
            Value::Json(_) | Value::Tuple(_) | Value::IntArray(_) | Value::FloatArray(_) => {
                let json = serialize_value_to_json(value)?;
                SqliteOwnedValue::Text(json.to_string())
            }
            Value::PyObjectWrapper(_) => SqliteOwnedValue::Text(create_bincoded_value(value)?),
            Value::Error | Value::Pending => {
                return Err(WriteError::UnsupportedType(Type::Any));
            }
        })
    }
}

impl Writer for SqliteWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        // Stream mode records every event, including deletions, with
        // their time/diff metadata. Snapshot mode handles +1 as an UPSERT
        // and -1 as a DELETE; other diffs are not emitted by the engine.
        self.buffer.push(data);
        if let Some(max) = self.max_batch_size {
            if self.buffer.len() >= max {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let tx = self.connection.transaction()?;
        for ctx in self.buffer.drain(..) {
            let storage: Vec<SqliteOwnedValue> = ctx
                .values
                .iter()
                .map(Self::value_to_sqlite)
                .collect::<Result<_, _>>()?;

            let params: Vec<SqliteOwnedValue> = if self.snapshot_mode {
                if ctx.diff > 0 {
                    storage
                } else {
                    self.query_template.primary_key_fields(storage)
                }
            } else {
                let mut params = storage;
                params.push(SqliteOwnedValue::Integer(ctx.time.0.try_into().map_err(
                    |_| WriteError::IntOutOfRange(ctx.time.0.try_into().unwrap_or(i64::MAX)),
                )?));
                params.push(SqliteOwnedValue::Integer(ctx.diff.try_into().map_err(
                    |_| WriteError::IntOutOfRange(ctx.diff.try_into().unwrap_or(i64::MAX)),
                )?));
                params
            };
            let bound: Vec<ToSqlOutput<'_>> = params
                .iter()
                .map(|v| ToSqlOutput::Borrowed(v.into()))
                .collect();
            tx.execute(
                &self.query_template.build_query(ctx.diff),
                params_from_iter(bound),
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn name(&self) -> String {
        format!("Sqlite({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}

// Copyright © 2026 Pathway

//! Storage for the memoization cache of non-deterministic expressions.
//!
//! Results of non-deterministic expressions (e.g. Python UDFs with
//! `deterministic=False`) are memoized so that a later retraction of a row
//! replays exactly the value produced originally. By default the memo lives
//! in in-memory hash maps inside the operator, so memory usage grows with
//! the number of live rows times the size of cached values.
//!
//! Passing `udf_cache_directory` to `pw.run` moves this working set to
//! per-operator `SQLite` files in the given directory, so that runtime
//! memory no longer scales with the number of cached values. When the
//! parameter is `None` (the default), the cache stays in memory.
//!
//! **Warning:** on many systems `/tmp` is a RAM-backed `tmpfs` — placing
//! the cache there brings the memory cost back through the page cache.
//! Point the cache directory at a real disk to actually save memory.
//!
//! The on-disk cache is a runtime working set only, NOT a durability
//! mechanism: operator snapshots remain the source of truth across restarts.
//! Every cache file is created from scratch when it is opened and removed on
//! graph teardown; a file left over by a crashed run is never read.

use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use bincode::{deserialize, serialize};
use log::{info, warn};
use rusqlite::{Connection, ErrorCode, OptionalExtension};

use crate::engine::error::DynResult;
use crate::engine::{Key, Value};

const CACHE_FILE_PREFIX: &str = "run-";
const CACHE_FILE_SUFFIX: &str = ".sqlite";

/// Memoization storage for the non-deterministic expressions of a single
/// `expression_table` operator. `expression_index` selects the cache of one
/// of the operator's expressions.
///
/// The caller wraps the processing of every minibatch in a
/// `begin_batch`/`commit_batch` pair so that backends can batch writes.
pub trait ExpressionCache {
    /// Starts the processing of a single minibatch.
    fn begin_batch(&mut self);
    /// Finishes the processing of a single minibatch.
    fn commit_batch(&mut self);
    /// Returns the cached value without removing it (append-only expressions).
    fn get(&mut self, expression_index: usize, key: Key) -> Option<Value>;
    /// Removes and returns the cached value.
    fn remove(&mut self, expression_index: usize, key: Key) -> Option<Value>;
    /// Inserts a value. Panics if the key is already cached - a cached key can
    /// only be seen again after its entry was removed by a deletion.
    fn insert(&mut self, expression_index: usize, key: Key, value: &Value);
}

/// Creates the memoization storage for the non-deterministic expressions of a
/// single `expression_table` operator: either in-memory hash maps (the
/// default) or a per-(operator, worker) `SQLite` file when `directory` is set
/// (the `udf_cache_directory` parameter of `pw.run`).
///
/// `next_operator_id` numbers the `SQLite`-backed caches so that each one
/// gets its own file; it is advanced only when such a cache is created.
pub fn create_expression_cache(
    should_cache: &[bool],
    directory: Option<&Path>,
    worker_index: usize,
    next_operator_id: &mut usize,
) -> DynResult<Box<dyn ExpressionCache>> {
    if should_cache.iter().any(|should_cache| *should_cache) {
        if let Some(directory) = directory {
            let operator_id = *next_operator_id;
            *next_operator_id += 1;
            let cache = SqliteExpressionCache::new(directory, worker_index, operator_id)?;
            return Ok(Box::new(cache));
        }
    }
    Ok(Box::new(InMemoryExpressionCache::new(should_cache.len())))
}

struct InMemoryExpressionCache {
    caches: Vec<HashMap<Key, Value>>,
}

impl InMemoryExpressionCache {
    fn new(n_expressions: usize) -> Self {
        let mut caches = Vec::with_capacity(n_expressions);
        caches.resize_with(n_expressions, HashMap::new);
        Self { caches }
    }
}

impl ExpressionCache for InMemoryExpressionCache {
    fn begin_batch(&mut self) {}

    fn commit_batch(&mut self) {}

    fn get(&mut self, expression_index: usize, key: Key) -> Option<Value> {
        self.caches[expression_index].get(&key).cloned()
    }

    fn remove(&mut self, expression_index: usize, key: Key) -> Option<Value> {
        self.caches[expression_index].remove(&key)
    }

    fn insert(&mut self, expression_index: usize, key: Key, value: &Value) {
        let current = self.caches[expression_index].insert(key, value.clone());
        assert!(
            current.is_none(),
            "expression cache already contains a value for key {key}"
        );
    }
}

/// Cache stored in a `SQLite` file. The file is owned exclusively by a single
/// (operator, worker) pair, is created empty and is removed when the operator
/// is dropped, so no cross-process locking is needed and any leftover content
/// from a previous run is never read.
struct SqliteExpressionCache {
    connection: Connection,
    path: PathBuf,
}

impl SqliteExpressionCache {
    fn new(directory: &Path, worker_index: usize, operator_id: usize) -> DynResult<Self> {
        fs::create_dir_all(directory)?;
        remove_stale_cache_files(directory);
        let path = directory.join(format!(
            "{CACHE_FILE_PREFIX}{pid}-worker-{worker_index}-op-{operator_id}{CACHE_FILE_SUFFIX}",
            pid = std::process::id(),
        ));
        // Never reuse the contents of an existing file: the on-disk cache is
        // not a durability mechanism, operator snapshots are.
        remove_file_if_exists(&path)?;
        let connection = Connection::open(&path)?;
        // The file is recreated from scratch on every run and written by a
        // single thread, so neither durability nor rollback support is
        // needed. Transactions are used only to batch writes.
        connection.pragma_update(None, "journal_mode", "OFF")?;
        connection.pragma_update(None, "synchronous", "OFF")?;
        connection.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        // A rowid table with a separate unique index is much faster to insert
        // into than a `WITHOUT ROWID` table here: keys are hashes, so primary
        // key-ordered storage would scatter the (potentially large) values
        // all over the tree, while a rowid table appends them in arrival
        // order and only the small index entries land at random positions.
        connection.execute_batch(
            "CREATE TABLE cache (
                expression_index INTEGER NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL
            );
            CREATE UNIQUE INDEX cache_key ON cache (expression_index, key);",
        )?;
        info!(
            "Storing the non-deterministic expression cache in {}",
            path.display()
        );
        Ok(Self { connection, path })
    }

    fn expression_index_param(expression_index: usize) -> i64 {
        i64::try_from(expression_index).expect("expression index should fit into i64")
    }
}

impl ExpressionCache for SqliteExpressionCache {
    fn begin_batch(&mut self) {
        self.connection
            .execute_batch("BEGIN")
            .expect("starting a transaction in the expression cache should succeed");
    }

    fn commit_batch(&mut self) {
        self.connection
            .execute_batch("COMMIT")
            .expect("committing a transaction in the expression cache should succeed");
    }

    fn get(&mut self, expression_index: usize, key: Key) -> Option<Value> {
        let mut statement = self
            .connection
            .prepare_cached("SELECT value FROM cache WHERE expression_index = ?1 AND key = ?2")
            .expect("preparing a query for the expression cache should succeed");
        statement
            .query_row(
                (
                    Self::expression_index_param(expression_index),
                    &key.0.to_le_bytes()[..],
                ),
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .expect("reading from the expression cache should succeed")
            .map(|serialized| {
                deserialize(&serialized).expect("cached value should be deserializable")
            })
    }

    fn remove(&mut self, expression_index: usize, key: Key) -> Option<Value> {
        let mut statement = self
            .connection
            .prepare_cached(
                "DELETE FROM cache WHERE expression_index = ?1 AND key = ?2 RETURNING value",
            )
            .expect("preparing a query for the expression cache should succeed");
        statement
            .query_row(
                (
                    Self::expression_index_param(expression_index),
                    &key.0.to_le_bytes()[..],
                ),
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .expect("removing from the expression cache should succeed")
            .map(|serialized| {
                deserialize(&serialized).expect("cached value should be deserializable")
            })
    }

    fn insert(&mut self, expression_index: usize, key: Key, value: &Value) {
        let serialized = serialize(value).expect("cached value should be serializable");
        let mut statement = self
            .connection
            .prepare_cached("INSERT INTO cache (expression_index, key, value) VALUES (?1, ?2, ?3)")
            .expect("preparing a query for the expression cache should succeed");
        let result = statement.execute((
            Self::expression_index_param(expression_index),
            &key.0.to_le_bytes()[..],
            serialized,
        ));
        match result {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == ErrorCode::ConstraintViolation =>
            {
                // Keep the same invariant as the in-memory implementation.
                panic!("expression cache already contains a value for key {key}");
            }
            Err(error) => {
                panic!("writing to the expression cache should succeed: {error}");
            }
        }
    }
}

impl Drop for SqliteExpressionCache {
    fn drop(&mut self) {
        // The file can be removed while the connection is still open - the
        // cache directory is required to be on a filesystem where unlinking
        // open files is allowed (any local filesystem on Unix).
        if let Err(error) = remove_file_if_exists(&self.path) {
            warn!(
                "Failed to remove the expression cache file {}: {error}",
                self.path.display()
            );
        }
    }
}

fn remove_file_if_exists(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Best-effort removal of cache files left over by crashed runs. Such files
/// are never read (every cache file is recreated from scratch when opened)
/// but they would leak disk space. A file is considered stale if the process
/// that created it is no longer alive.
fn remove_stale_cache_files(directory: &Path) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(pid) = file_name
            .strip_prefix(CACHE_FILE_PREFIX)
            .and_then(|rest| rest.split('-').next())
            .and_then(|pid| pid.parse::<u32>().ok())
        else {
            continue;
        };
        if !file_name.ends_with(CACHE_FILE_SUFFIX)
            || pid == std::process::id()
            || process_is_alive(pid)
        {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => info!(
                "Removed a stale expression cache file {}",
                entry.path().display()
            ),
            Err(error) if error.kind() == ErrorKind::NotFound => {} // lost a race with another worker
            Err(error) => warn!(
                "Failed to remove a stale expression cache file {}: {error}",
                entry.path().display()
            ),
        }
    }
}

#[cfg(target_os = "linux")]
fn process_is_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

#[cfg(not(target_os = "linux"))]
fn process_is_alive(_pid: u32) -> bool {
    true // conservative: never treat files of other runs as stale
}

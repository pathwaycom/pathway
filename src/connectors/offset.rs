use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::engine::value::HashInto;

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum OffsetKey {
    Kafka(Arc<String>, i32),
    Empty,
}

impl HashInto for OffsetKey {
    fn hash_into(&self, hasher: &mut Hasher) {
        match self {
            OffsetKey::Kafka(topic_name, partition) => {
                hasher.update(topic_name.as_bytes());
                partition.hash_into(hasher);
            }
            OffsetKey::Empty => {}
        };
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum OffsetValue {
    KafkaOffset(i64),
    FilePosition {
        total_entries_read: u64,
        path: Arc<PathBuf>,
        bytes_offset: u64,
    },
    S3ObjectPosition {
        total_entries_read: u64,
        path: Arc<String>,
        bytes_offset: u64,
    },
    PythonEntrySequentialId(u64),
    Empty,
}

impl HashInto for OffsetValue {
    fn hash_into(&self, hasher: &mut Hasher) {
        match self {
            OffsetValue::KafkaOffset(offset) => offset.hash_into(hasher),
            OffsetValue::FilePosition {
                path, bytes_offset, ..
            } => {
                hasher.update(path.as_os_str().as_bytes());
                bytes_offset.hash_into(hasher);
            }
            OffsetValue::S3ObjectPosition {
                path, bytes_offset, ..
            } => {
                hasher.update(path.as_bytes());
                bytes_offset.hash_into(hasher);
            }
            OffsetValue::PythonEntrySequentialId(sequential_id) => {
                sequential_id.hash_into(hasher);
            }
            OffsetValue::Empty => {}
        };
    }
}

pub type Offset = (OffsetKey, OffsetValue);

/// Empty offset for connectors that don't support persistence
pub const EMPTY_OFFSET: Offset = (OffsetKey::Empty, OffsetValue::Empty);

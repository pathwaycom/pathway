// Copyright © 2024 Pathway

use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::Arc;

use arcstr::ArcStr;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::connectors::data_lake::iceberg::IcebergSnapshotId;
use crate::engine::value::HashInto;
use crate::persistence::cached_object_storage::CachedObjectVersion;

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum OffsetKey {
    Kafka(ArcStr, i32),
    Nats(usize),
    Empty,
}

impl HashInto for OffsetKey {
    fn hash_into(&self, hasher: &mut Hasher) {
        match self {
            OffsetKey::Kafka(topic_name, partition) => {
                hasher.update(topic_name.as_bytes());
                partition.hash_into(hasher);
            }
            OffsetKey::Nats(worker_index) => worker_index.hash_into(hasher),
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
        path: ArcStr,
        bytes_offset: u64,
    },
    PosixLikeOffset {
        total_entries_read: u64,
        path: Arc<[u8]>,
        bytes_offset: u64, // Not used by persistence, but used to autogenerate primary key
        cached_object_version: Option<CachedObjectVersion>,
    },
    PythonCursor {
        raw_external_offset: Arc<[u8]>,
        total_entries_read: u64,
    },
    DeltaTablePosition {
        version: i64,
        rows_read_within_version: i64,
    },
    IcebergSnapshot {
        snapshot_id: IcebergSnapshotId,
    },
    NatsReadEntriesCount(usize),
    Empty,
}

impl OffsetValue {
    pub fn as_posix_like_offset(&self) -> Option<OffsetValue> {
        match self {
            Self::PosixLikeOffset { .. } => Some(self.clone()),
            Self::FilePosition {
                total_entries_read,
                path,
                bytes_offset,
            } => Some(Self::PosixLikeOffset {
                total_entries_read: *total_entries_read,
                path: path.as_os_str().as_bytes().into(),
                bytes_offset: *bytes_offset,
                cached_object_version: None,
            }),
            Self::S3ObjectPosition {
                total_entries_read,
                path,
                bytes_offset,
            } => Some(Self::PosixLikeOffset {
                total_entries_read: *total_entries_read,
                path: path.as_bytes().into(),
                bytes_offset: *bytes_offset,
                cached_object_version: None,
            }),
            _ => None,
        }
    }
}

/// Used to autogenerate row primary key
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
            OffsetValue::PosixLikeOffset {
                path, bytes_offset, ..
            } => {
                hasher.update(path);
                bytes_offset.hash_into(hasher);
            }
            OffsetValue::PythonCursor {
                total_entries_read,
                raw_external_offset,
            } => {
                total_entries_read.hash_into(hasher);
                hasher.update(raw_external_offset);
            }
            OffsetValue::DeltaTablePosition {
                version,
                rows_read_within_version,
            } => {
                version.hash_into(hasher);
                rows_read_within_version.hash_into(hasher);
            }
            OffsetValue::NatsReadEntriesCount(count) => count.hash_into(hasher),
            OffsetValue::IcebergSnapshot { snapshot_id } => {
                snapshot_id.hash_into(hasher);
            }
            OffsetValue::Empty => {}
        };
    }
}

pub type Offset = (OffsetKey, OffsetValue);

/// Empty offset for connectors that don't support persistence
pub const EMPTY_OFFSET: Offset = (OffsetKey::Empty, OffsetValue::Empty);

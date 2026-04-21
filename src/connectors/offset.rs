// Copyright © 2026 Pathway

use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::Arc;

use arcstr::ArcStr;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::connectors::data_lake::iceberg::IcebergSnapshotId;
use crate::engine::value::HashInto;
use crate::persistence::cached_object_storage::CachedObjectVersion;

/// Identifies the type of `RabbitMQ` stream for offset tracking.
///
/// This is `#[non_exhaustive]` to allow adding Super Streams support later
/// (where each partition is a separate physical stream) without breaking
/// serialized persistence snapshots.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
#[non_exhaustive]
pub enum RabbitmqStreamType {
    /// A plain `RabbitMQ` stream identified by name.
    Stream(ArcStr),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum OffsetKey {
    Kafka(ArcStr, i32),
    Nats(usize),
    Empty,
    Kinesis(ArcStr),
    MongoDb,
    Rabbitmq(RabbitmqStreamType),
    Mssql,
}

impl HashInto for OffsetKey {
    fn hash_into(&self, hasher: &mut Hasher) {
        match self {
            OffsetKey::Kafka(topic_name, partition) => {
                hasher.update(topic_name.as_bytes());
                partition.hash_into(hasher);
            }
            OffsetKey::Nats(worker_index) => {
                worker_index.hash_into(hasher);
            }
            OffsetKey::Rabbitmq(RabbitmqStreamType::Stream(stream_name)) => {
                hasher.update(stream_name.as_bytes());
            }
            OffsetKey::Kinesis(shard) => hasher.update(shard.as_bytes()),
            OffsetKey::Empty | OffsetKey::MongoDb | OffsetKey::Mssql => {}
        }
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
    MqttReadEntriesCount(usize),
    PostgresReadEntriesCount(usize),
    KinesisOffset(String),
    /// Raw `BSON` bytes of a `MongoDB` change-stream resume token.
    /// `MongoDB` guarantees that tokens are lexicographically ordered by oplog position,
    /// so byte-wise comparison is a valid ordering.
    MongoDbOplogToken(Vec<u8>),
    Empty,
    RabbitmqOffset(u64),
    /// Raw bytes of a SQL Server Change Data Capture Log Sequence Number
    /// (10-byte fixed-width big-endian triple of VLF sequence / log block / slot).
    /// `fn_cdc_get_max_lsn` returns LSNs in this exact binary layout.  Because
    /// SQL Server guarantees LSNs are monotonically increasing and fixed-width,
    /// byte-wise comparison is a valid ordering — the same strategy used for
    /// [`Self::MongoDbOplogToken`].
    MssqlCdcLsn(Vec<u8>),
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
            OffsetValue::NatsReadEntriesCount(count)
            | OffsetValue::MqttReadEntriesCount(count)
            | OffsetValue::PostgresReadEntriesCount(count) => {
                count.hash_into(hasher);
            }
            OffsetValue::RabbitmqOffset(offset) => {
                offset.hash_into(hasher);
            }
            OffsetValue::IcebergSnapshot { snapshot_id } => {
                snapshot_id.hash_into(hasher);
            }
            OffsetValue::KinesisOffset(offset) => {
                offset.hash_into(hasher);
            }
            OffsetValue::MongoDbOplogToken(bytes) | OffsetValue::MssqlCdcLsn(bytes) => {
                hasher.update(bytes);
            }
            OffsetValue::Empty => {}
        }
    }
}

pub type Offset = (OffsetKey, OffsetValue);

/// Empty offset for connectors that don't support persistence
pub const EMPTY_OFFSET: Offset = (OffsetKey::Empty, OffsetValue::Empty);

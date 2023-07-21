use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize, Ord, PartialOrd)]
pub enum OffsetKey {
    Kafka(Arc<String>, i32),
    Empty,
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
}

pub type Offset = (OffsetKey, OffsetValue);

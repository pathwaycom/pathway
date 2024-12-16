// Copyright © 2024 Pathway

use log::error;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::DateTime;
use s3::serde_types::Object as S3Object;
use serde::{Deserialize, Serialize};

use crate::timestamp::current_unix_timestamp_secs;

/// Basic metadata for a file-like object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, Ord, PartialOrd)]
pub struct FileLikeMetadata {
    // Creation and modification time may not be available at some platforms
    // Stored in u64 for easy serialization
    created_at: Option<u64>,
    pub modified_at: Option<u64>,

    // Owner may be unavailable at some platforms
    owner: Option<String>,

    // Path should always be available. We make it String for two reasons:
    // * S3 path is denoted as a String
    // * This object is directly serialized and passed into a connector row
    pub path: String,

    // Size (in bytes) should be always available.
    pub size: u64,

    // Record acquisition time. Required for the real-time indexer processes
    // to determine the gap between finding file and indexing it.
    seen_at: u64,
}

impl FileLikeMetadata {
    pub fn from_fs_meta(path: &Path, meta: &std::fs::Metadata) -> Self {
        let created_at = metadata_time_to_unix_timestamp(meta.created().ok());
        let modified_at = metadata_time_to_unix_timestamp(meta.modified().ok());
        let owner = file_owner::get_owner(meta);

        Self {
            created_at,
            modified_at,
            owner,
            path: path.to_string_lossy().to_string(),
            size: meta.len(),
            seen_at: current_unix_timestamp_secs(),
        }
    }

    pub fn from_s3_object(object: &S3Object) -> Self {
        let modified_at: Option<u64> = match DateTime::parse_from_rfc3339(&object.last_modified) {
            Ok(last_modified) => {
                if let Ok(last_modified) = last_modified.timestamp().try_into() {
                    Some(last_modified)
                } else {
                    error!("S3 modification time is not a UNIX timestamp: {last_modified}");
                    None
                }
            }
            Err(e) => {
                error!(
                    "Failed to parse RFC 3339 timestamp '{}' from S3 metadata: {e}",
                    object.last_modified
                );
                None
            }
        };

        Self {
            created_at: None,
            modified_at,
            owner: object.owner.as_ref().map(|owner| owner.id.clone()),
            path: object.key.clone(),
            size: object.size,
            seen_at: current_unix_timestamp_secs(),
        }
    }

    /// Checks if file contents could have been changed.
    pub fn is_changed(&self, other: &FileLikeMetadata) -> bool {
        self.modified_at != other.modified_at
            || self.size != other.size
            || self.owner != other.owner
    }
}

#[cfg(target_os = "linux")]
mod file_owner {
    use nix::unistd::User;
    use std::os::unix::fs::MetadataExt;

    pub fn get_owner(metadata: &std::fs::Metadata) -> Option<String> {
        let uid = metadata.uid();
        let user = User::from_uid(uid.into()).ok()?;
        Some(user?.name)
    }
}

#[cfg(not(target_os = "linux"))]
mod file_owner {
    pub fn get_owner(_metadata: &std::fs::Metadata) -> Option<String> {
        None
    }
}

fn metadata_time_to_unix_timestamp(timestamp: Option<SystemTime>) -> Option<u64> {
    timestamp
        .and_then(|timestamp| timestamp.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
}

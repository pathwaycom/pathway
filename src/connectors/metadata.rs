use log::error;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Basic metadata for a file-like object
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceMetadata {
    // Creation and modification time may not be available at some platforms
    // Stored in u64 for easy serialization
    created_at: Option<u64>,
    modified_at: Option<u64>,

    // Owner may be unavailable at some platforms and on S3
    owner: Option<String>,

    // Path should always be available. We make it String for two reasons:
    // * S3 path is denoted as a String
    // * This object is directly serialized and passed into a connector row
    path: String,
}

impl SourceMetadata {
    pub fn from_fs_path(path: &Path) -> Self {
        let (created_at, modified_at, owner) = match std::fs::metadata(path) {
            Ok(metadata) => (
                metadata_time_to_unix_timestamp(metadata.created().ok()),
                metadata_time_to_unix_timestamp(metadata.modified().ok()),
                file_owner::get_owner(&metadata),
            ),
            Err(e) => {
                error!("Failed to get metadata for filesystem object {path:?}, details: {e}");
                (None, None, None)
            }
        };

        Self {
            created_at,
            modified_at,
            owner,
            path: path.to_string_lossy().to_string(),
        }
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
    pub fn get_owner(metadata: &std::fs::Metadata) -> Option<String> {
        None
    }
}

fn metadata_time_to_unix_timestamp(timestamp: Option<SystemTime>) -> Option<u64> {
    timestamp
        .and_then(|timestamp| timestamp.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
}

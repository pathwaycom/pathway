// Copyright © 2026 Pathway

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
    use log::{error, warn};
    use nix::unistd::User;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::os::unix::fs::MetadataExt;
    use std::time::{Duration, Instant};

    use crate::retry::{execute_with_retries, RetryConfig};

    const ERROR_THROTTLE_INTERVAL_PER_UID: Duration = Duration::from_mins(1);

    // NSS configuration can change at runtime (e.g. sssd coming up), so a
    // "UID not found" answer is only cached for a limited time. Positive
    // entries are kept forever.
    const NEGATIVE_CACHE_TTL: Duration = Duration::from_mins(1);

    enum CachedResolution {
        Found(String),
        NotFound(Instant),
    }

    thread_local! {
        static UID_USER_CACHE: RefCell<HashMap<u32, CachedResolution>> = RefCell::new(HashMap::new());
        static LAST_ERROR_REPORTED_AT: RefCell<HashMap<u32, Instant>> = RefCell::new(HashMap::new());
    }

    fn need_to_report_error(uid: u32) -> bool {
        LAST_ERROR_REPORTED_AT.with(|last_reported| {
            last_reported
                .borrow()
                .get(&uid)
                .is_none_or(|&last_time| last_time.elapsed() >= ERROR_THROTTLE_INTERVAL_PER_UID)
        })
    }

    fn mark_error_reported(uid: u32) {
        LAST_ERROR_REPORTED_AT.with(|last_reported| {
            last_reported.borrow_mut().insert(uid, Instant::now());
        });
    }

    fn cache_resolution(uid: u32, resolution: CachedResolution) {
        UID_USER_CACHE.with(|cache| {
            cache.borrow_mut().insert(uid, resolution);
        });
    }

    /// Returns the still-valid cached entry for the UID, if any. Expired
    /// negative entries are evicted and reported as a cache miss.
    fn get_cached_resolution(uid: u32) -> Option<CachedResolution> {
        UID_USER_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            match cache.get(&uid) {
                Some(CachedResolution::Found(name)) => Some(CachedResolution::Found(name.clone())),
                Some(CachedResolution::NotFound(cached_at)) => {
                    if cached_at.elapsed() < NEGATIVE_CACHE_TTL {
                        Some(CachedResolution::NotFound(*cached_at))
                    } else {
                        cache.remove(&uid);
                        None
                    }
                }
                None => None,
            }
        })
    }

    fn get_owner_impl(
        uid: u32,
        resolve: impl FnMut() -> nix::Result<Option<String>>,
    ) -> Option<String> {
        match get_cached_resolution(uid) {
            Some(CachedResolution::Found(name)) => return Some(name),
            Some(CachedResolution::NotFound(_)) => return None,
            None => {}
        }

        // Retries handle transient resolver errors (EINTR/EAGAIN/NSS
        // hiccups); a clean "not found" is `Ok(None)` and returns from
        // `execute_with_retries` immediately, without retry sleeps.
        let user = execute_with_retries(
            resolve,
            RetryConfig::new(Duration::from_millis(50), 1.5, Duration::from_millis(10)),
            5,
        );
        match user {
            Ok(Some(name)) => {
                cache_resolution(uid, CachedResolution::Found(name.clone()));
                Some(name)
            }
            Ok(None) => {
                cache_resolution(uid, CachedResolution::NotFound(Instant::now()));
                if need_to_report_error(uid) {
                    warn!("UID {uid} not found in system user database");
                    mark_error_reported(uid);
                }
                None
            }
            Err(err) => {
                // Errors are transient by assumption and aren't cached.
                if need_to_report_error(uid) {
                    error!("Failed to resolve user name for UID {uid}: {err}");
                    mark_error_reported(uid);
                }
                None
            }
        }
    }

    pub fn get_owner(metadata: &std::fs::Metadata) -> Option<String> {
        let uid = metadata.uid();
        get_owner_impl(
            uid,
            || Ok(User::from_uid(uid.into())?.map(|user| user.name)),
        )
    }

    #[cfg(test)]
    mod tests {
        use super::{get_owner_impl, ERROR_THROTTLE_INTERVAL_PER_UID, NEGATIVE_CACHE_TTL};

        // Caches are thread-local and each test runs on its own thread, but
        // distinct UIDs keep the tests independent regardless of the runner.
        const UNMAPPED_UID: u32 = 3_000_000_001;
        const MAPPED_UID: u32 = 3_000_000_002;
        const FAILING_UID: u32 = 3_000_000_003;

        #[test]
        fn test_unmapped_uid_is_resolved_once_per_ttl() {
            assert!(NEGATIVE_CACHE_TTL <= ERROR_THROTTLE_INTERVAL_PER_UID);
            let mut resolver_calls = 0;
            for _ in 0..100 {
                let owner = get_owner_impl(UNMAPPED_UID, || {
                    resolver_calls += 1;
                    Ok(None)
                });
                assert_eq!(owner, None);
            }
            assert_eq!(resolver_calls, 1);
        }

        #[test]
        fn test_mapped_uid_is_resolved_once_and_cached() {
            let mut resolver_calls = 0;
            for _ in 0..100 {
                let owner = get_owner_impl(MAPPED_UID, || {
                    resolver_calls += 1;
                    Ok(Some("pathway".to_string()))
                });
                assert_eq!(owner, Some("pathway".to_string()));
            }
            assert_eq!(resolver_calls, 1);
        }

        #[test]
        fn test_resolver_errors_are_retried_and_not_cached() {
            let mut resolver_calls = 0;
            let owner = get_owner_impl(FAILING_UID, || {
                resolver_calls += 1;
                Err(nix::errno::Errno::EAGAIN)
            });
            assert_eq!(owner, None);
            assert_eq!(resolver_calls, 6); // the initial attempt + 5 retries

            // A subsequent successful resolution isn't shadowed by the failure.
            let owner = get_owner_impl(FAILING_UID, || Ok(Some("pathway".to_string())));
            assert_eq!(owner, Some("pathway".to_string()));
        }
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

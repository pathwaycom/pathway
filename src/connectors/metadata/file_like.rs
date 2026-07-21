// Copyright © 2026 Pathway

use log::error;
use std::collections::HashMap;
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

    pub mtime_ns: Option<u64>,
    pub etag: Option<String>,
    pub inode: Option<u64>,
    pub ctime_ns: Option<u64>,
}

impl FileLikeMetadata {
    pub fn from_fs_meta(path: &Path, meta: &std::fs::Metadata) -> Self {
        let created_at = metadata_time_to_unix_timestamp(meta.created().ok());
        let modified_at = metadata_time_to_unix_timestamp(meta.modified().ok());
        let owner = file_owner::get_owner(meta);

        let mtime_ns = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .or_else(|| modified_at.map(|m| m * 1_000_000_000));

        #[cfg(unix)]
        let (inode, ctime_ns) = {
            use std::os::unix::fs::MetadataExt;
            let ctime_seconds = u64::try_from(meta.ctime()).unwrap_or(0);
            let ctime_nanoseconds = u64::try_from(meta.ctime_nsec()).unwrap_or(0);
            let ctime_ns = if meta.ctime() >= 0 {
                Some(ctime_seconds * 1_000_000_000 + ctime_nanoseconds)
            } else {
                None
            };
            (Some(meta.ino()), ctime_ns)
        };
        #[cfg(not(unix))]
        let (inode, ctime_ns) = (None, None);

        Self {
            created_at,
            modified_at,
            owner,
            path: path.to_string_lossy().to_string(),
            size: meta.len(),
            seen_at: current_unix_timestamp_secs(),
            mtime_ns,
            etag: None,
            inode,
            ctime_ns,
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
            mtime_ns: None,
            etag: object.e_tag.clone(),
            inode: None,
            ctime_ns: None,
        }
    }

    /// Constructs a `FileLikeMetadata` from a V1 (legacy) deserialized record,
    /// setting all fields introduced after V1 to `None`.
    pub fn from_v1(
        created_at: Option<u64>,
        modified_at: Option<u64>,
        owner: Option<String>,
        path: String,
        size: u64,
        seen_at: u64,
    ) -> Self {
        Self {
            created_at,
            modified_at,
            owner,
            path,
            size,
            seen_at,
            mtime_ns: None,
            etag: None,
            inode: None,
            ctime_ns: None,
        }
    }

    /// Checks if file contents could have been changed.
    ///
    /// `ScannerTag` and `OwnerInterner::is_changed` mirror exactly the fields
    /// compared here. If this method changes, they must be updated in sync.
    pub fn is_changed(&self, other: &FileLikeMetadata) -> bool {
        self.modified_at != other.modified_at
            || self.size != other.size
            || self.owner != other.owner
            || self.mtime_ns != other.mtime_ns
            || self.etag != other.etag
            || self.inode != other.inode
            || self.ctime_ns != other.ctime_ns
    }
}

/// A compact digest of `FileLikeMetadata` holding only the fields that
/// `FileLikeMetadata::is_changed` compares. The posix-like scanners keep one
/// tag per watched object in RAM, so it must stay small and heap-free: the
/// string identifiers (owner/etag) are replaced by an id interned in
/// `IdentityInterner`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScannerTag {
    Posix {
        mtime_ns: u64, // Fallback to modified_at * 1_000_000_000 if absent
        size: u64,
        identity_id: u32,
        inode: u64,
        ctime_ns: u64,
    },
    S3 {
        last_modified: u64,
        size: u64,
        identity_id: u32,
    },
}

const _: () = assert!(std::mem::size_of::<ScannerTag>() == 40);

const NO_IDENTITY_ID: u32 = 0;

/// Maps identity strings (owner or etag) to compact ids for `ScannerTag`.
#[derive(Debug, Default)]
pub struct IdentityInterner {
    identity_ids: HashMap<String, u32>,
}

impl IdentityInterner {
    /// Builds the in-RAM tag for a metadata entry, interning its identity.
    pub fn tag(&mut self, metadata: &FileLikeMetadata) -> ScannerTag {
        if metadata.path.starts_with("s3://")
            || metadata.path.starts_with("s3a://")
            || metadata.etag.is_some()
        {
            ScannerTag::S3 {
                last_modified: metadata.modified_at.unwrap_or(0),
                size: metadata.size,
                identity_id: self.intern_identity(metadata.etag.as_deref()),
            }
        } else {
            let mtime_ns = metadata
                .mtime_ns
                .unwrap_or_else(|| metadata.modified_at.unwrap_or(0) * 1_000_000_000);
            ScannerTag::Posix {
                mtime_ns,
                size: metadata.size,
                identity_id: self.intern_identity(metadata.owner.as_deref()),
                inode: metadata.inode.unwrap_or(0),
                ctime_ns: metadata.ctime_ns.unwrap_or(0),
            }
        }
    }

    /// Mirrors `FileLikeMetadata::is_changed` for a stored tag and the actual
    /// metadata of the object.
    pub fn is_changed(&self, stored: &ScannerTag, actual: &FileLikeMetadata) -> bool {
        match stored {
            ScannerTag::Posix {
                mtime_ns,
                size,
                identity_id,
                inode,
                ctime_ns,
            } => {
                let actual_mtime_ns = actual
                    .mtime_ns
                    .unwrap_or_else(|| actual.modified_at.unwrap_or(0) * 1_000_000_000);
                *mtime_ns != actual_mtime_ns
                    || *size != actual.size
                    || *inode != actual.inode.unwrap_or(0)
                    || *ctime_ns != actual.ctime_ns.unwrap_or(0)
                    || self.lookup_identity(actual.owner.as_deref()) != Some(*identity_id)
            }
            ScannerTag::S3 {
                last_modified,
                size,
                identity_id,
            } => {
                *last_modified != actual.modified_at.unwrap_or(0)
                    || *size != actual.size
                    || self.lookup_identity(actual.etag.as_deref()) != Some(*identity_id)
            }
        }
    }

    fn intern_identity(&mut self, identity: Option<&str>) -> u32 {
        let Some(identity) = identity else {
            return NO_IDENTITY_ID;
        };
        if let Some(id) = self.identity_ids.get(identity) {
            return *id;
        }
        let id = u32::try_from(self.identity_ids.len() + 1).expect("too many distinct identities");
        self.identity_ids.insert(identity.to_string(), id);
        id
    }

    fn lookup_identity(&self, identity: Option<&str>) -> Option<u32> {
        match identity {
            None => Some(NO_IDENTITY_ID),
            Some(identity) => self.identity_ids.get(identity).copied(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(modified_at: Option<u64>, size: u64, owner: Option<&str>) -> FileLikeMetadata {
        FileLikeMetadata {
            created_at: None,
            modified_at,
            owner: owner.map(String::from),
            path: "/data/file.txt".to_string(),
            size,
            seen_at: 0,
            mtime_ns: None,
            etag: None,
            inode: None,
            ctime_ns: None,
        }
    }

    #[test]
    fn test_scanner_tag_mirrors_is_changed() {
        let mut interner = IdentityInterner::default();
        let cases = [
            metadata(Some(10), 4, Some("alice")),
            metadata(Some(10), 4, Some("bob")),
            metadata(Some(10), 4, None),
            metadata(Some(11), 4, Some("alice")),
            metadata(Some(10), 5, Some("alice")),
            metadata(None, 4, Some("alice")),
            // `Some(0)` must stay distinct from `None`: the tag stores the
            // missing modification time as 0 plus a separate flag.
            metadata(Some(0), 4, Some("alice")),
        ];
        let tags: Vec<_> = cases.iter().map(|m| interner.tag(m)).collect();
        for (stored, tag) in cases.iter().zip(&tags) {
            for actual in &cases {
                assert_eq!(
                    interner.is_changed(tag, actual),
                    stored.is_changed(actual),
                    "tag comparison diverged from FileLikeMetadata::is_changed for {stored:?} vs {actual:?}",
                );
            }
        }
    }

    #[test]
    fn test_scanner_tag_unknown_owner_counts_as_changed() {
        let mut interner = IdentityInterner::default();
        let stored = metadata(Some(10), 4, Some("alice"));
        let tag = interner.tag(&stored);
        // An owner string never seen by the interner can't match any stored id.
        assert!(interner.is_changed(&tag, &metadata(Some(10), 4, Some("charlie"))));
        assert!(interner.is_changed(&tag, &metadata(Some(10), 4, None)));
        assert!(!interner.is_changed(&tag, &metadata(Some(10), 4, Some("alice"))));

        let stored_ownerless = metadata(Some(10), 4, None);
        let tag_ownerless = interner.tag(&stored_ownerless);
        assert!(interner.is_changed(&tag_ownerless, &metadata(Some(10), 4, Some("charlie"))));
        assert!(!interner.is_changed(&tag_ownerless, &metadata(Some(10), 4, None)));
    }
}
